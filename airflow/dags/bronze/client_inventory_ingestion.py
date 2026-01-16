"""
Bronze Layer: Client Inventory Data Ingestion

Ingests inventory data from client's on-prem SQL Server database
via site-to-site VPN connection.

Schedule: Daily
Path: bronze/client/{table}/{YYYY-MM-DD-HH}/data.json

Manual Trigger Options (pass as conf JSON):
    - full_refresh: true     -> Force full refresh (ignore last sync timestamp)
    - tables: ["table1"]     -> Only sync specific tables (by target_table name)

Examples:
    # Full refresh all tables
    airflow dags trigger bronze_client_inventory_ingestion --conf '{"full_refresh": true}'

    # Refresh specific tables only
    airflow dags trigger bronze_client_inventory_ingestion --conf '{"tables": ["inv_item_mst"]}'
"""
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator

from lib.client_db import fetch_query
from lib.datalake import write_to_datalake, write_metadata
from lib.utils import get_date_hour
from lib.sql_templates import render_sql
from lib.sync_metadata import (
    get_last_sync_value,
    update_sync_metadata,
    format_sync_value_for_sql,
)
from lib.tables import IngestionPattern
from lib.tables.client import ALL_TABLES


def ingest_table(table_config, **context) -> Dict[str, Any]:
    """
    Ingest a single table based on its configuration.

    Args:
        table_config: TableConfig object defining the table
        **context: Airflow context

    Returns:
        Dict with ingestion results
    """
    partition = get_date_hour(context)
    table_name = table_config.target_table
    start_time = time.time()

    # Get manual trigger parameters
    dag_conf = context.get("dag_run").conf or {}
    force_full_refresh = dag_conf.get("full_refresh", False)
    selected_tables = dag_conf.get("tables", None)

    # Skip if specific tables requested and this isn't one of them
    if selected_tables and table_name not in selected_tables:
        print(f"Skipping {table_name} (not in selected tables: {selected_tables})")
        return {'table': table_name, 'skipped': True, 'reason': 'not_selected'}

    print(f"Starting ingestion for {table_config.full_source_name}")
    print(f"  Pattern: {table_config.pattern.value}")
    if force_full_refresh:
        print(f"  Mode: FULL REFRESH (forced via manual trigger)")

    # Build SQL query based on ingestion pattern
    # If force_full_refresh, use FULL_REFRESH pattern regardless of table config
    effective_pattern = (
        IngestionPattern.FULL_REFRESH if force_full_refresh
        else table_config.pattern
    )

    if effective_pattern == IngestionPattern.TIMESTAMP_INCREMENTAL:
        last_value = get_last_sync_value(table_name)
        last_value_str = format_sync_value_for_sql(last_value)

        print(f"  Last sync value: {last_value_str}")

        sql = render_sql(
            'sqlserver/bronze/incremental.sql.j2',
            schema=table_config.schema,
            table=table_config.source_table,
            timestamp_column=table_config.pattern_params['timestamp_column'],
            last_sync_value=last_value_str
        )

    elif effective_pattern == IngestionPattern.DATE_PARTITION:
        partition_date = context['ds']  # Use execution date as partition value
        sql = render_sql(
            'sqlserver/bronze/date_partition.sql.j2',
            schema=table_config.schema,
            table=table_config.source_table,
            partition_column=table_config.pattern_params['partition_column'],
            partition_value=partition_date
        )

    else:  # FULL_REFRESH
        sql = render_sql(
            'sqlserver/bronze/full_refresh.sql.j2',
            schema=table_config.schema,
            table=table_config.source_table
        )

    print(f"  Executing query...")

    # Execute query against client SQL Server
    rows = fetch_query(sql)
    row_count = len(rows)

    print(f"  Fetched {row_count} rows")

    if row_count == 0:
        print(f"  No new data to sync")
        return {
            'table': table_name,
            'rows_synced': 0,
            'skipped': True
        }

    # Write to Data Lake
    path = f"client/{table_name}/{partition}/data.json"
    json_data = json.dumps(rows, indent=2, default=str)
    write_to_datalake('bronze', path, json_data)

    # Write lineage metadata
    write_metadata('bronze', path, extra={
        'source': 'client_sqlserver',
        'source_table': table_config.full_source_name,
        'dataset': table_name,
        'partition': partition,
        'record_count': row_count,
        'pattern': table_config.pattern.value,
    })

    duration = time.time() - start_time

    # Update sync metadata for incremental tables
    if table_config.pattern == IngestionPattern.TIMESTAMP_INCREMENTAL:
        ts_column = table_config.pattern_params['timestamp_column']
        # Find max timestamp from synced rows
        max_ts_values = [r[ts_column] for r in rows if r.get(ts_column)]
        if max_ts_values:
            max_ts = max(max_ts_values)
            if isinstance(max_ts, str):
                max_ts = datetime.fromisoformat(max_ts)
            update_sync_metadata(
                table_name=table_name,
                last_sync_value=max_ts,
                rows_synced=row_count,
                sync_type=table_config.pattern.value,
                duration_seconds=duration,
                extra={
                    'partition': partition,
                    'path': f"bronze/{path}"
                }
            )

    print(f"  Ingestion complete: {row_count} records → bronze/{path}")
    print(f"  Duration: {duration:.2f}s")

    return {
        'table': table_name,
        'path': path,
        'rows_synced': row_count,
        'duration_seconds': round(duration, 2)
    }


# =============================================================================
# DAG DEFINITION
# =============================================================================
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bronze_client_inventory_ingestion',
    default_args=default_args,
    description='Bronze: Ingest inventory data from client SQL Server',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'client', 'inventory', 'medallion', 'incremental'],
    doc_md=__doc__,
    params={
        'full_refresh': Param(False, type='boolean', description='Force full refresh (ignore last sync timestamp)'),
        'tables': Param(None, type=['null', 'array'], description='Specific tables to sync (by target_table name)'),
    },
) as dag:

    # Create a task for each client table
    for table in ALL_TABLES:
        PythonOperator(
            task_id=f'ingest_{table.target_table}',
            python_callable=ingest_table,
            op_kwargs={'table_config': table},
        )
