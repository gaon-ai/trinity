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
    airflow dags trigger bronze_client_inventory_ingestion --conf '{"tables": ["invoice_item"]}'
"""
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from lib.client_db import fetch_query
from lib.utils import get_date_hour
from lib.ingestion import ingest_table
from lib.tables.client import ALL_TABLES
from lib.datasets import BRONZE_CLIENT_DATA


def _ingest_table_task(table_config, **context) -> Dict[str, Any]:
    """
    Task wrapper for ingest_table that handles DAG-specific logic.

    Checks for manual trigger parameters (full_refresh, tables filter)
    and delegates to the reusable ingestion function.
    """
    partition = get_date_hour(context)
    table_name = table_config.target_table

    # Get manual trigger parameters
    dag_conf = context.get("dag_run").conf or {}
    force_full_refresh = dag_conf.get("full_refresh", False)
    selected_tables = dag_conf.get("tables", None)

    # Skip if specific tables requested and this isn't one of them
    if selected_tables and table_name not in selected_tables:
        print(f"Skipping {table_name} (not in selected tables: {selected_tables})")
        return {'table': table_name, 'skipped': True, 'reason': 'not_selected'}

    # Use the centralized ingestion function
    return ingest_table(
        table_config=table_config,
        partition=partition,
        fetch_fn=fetch_query,
        force_full_refresh=force_full_refresh,
        source_name='client_sqlserver',
    )


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
        'full_refresh': Param(False, type='boolean', description='Force full refresh'),
        'tables': Param(None, type=['null', 'array'], description='Specific tables to sync'),
    },
) as dag:

    # Create a task for each client table
    ingest_tasks = []
    for table in ALL_TABLES:
        # Large tables get longer execution timeout
        execution_timeout = (
            timedelta(minutes=30) if table.target_table == 'general_ledger'
            else timedelta(minutes=10)
        )
        task = PythonOperator(
            task_id=f'ingest_{table.target_table}',
            python_callable=_ingest_table_task,
            op_kwargs={'table_config': table},
            execution_timeout=execution_timeout,
        )
        ingest_tasks.append(task)

    # Final task to signal completion and trigger downstream gold DAG
    complete = EmptyOperator(
        task_id='ingestion_complete',
        outlets=[BRONZE_CLIENT_DATA],
    )

    # All ingest tasks must complete before signaling completion
    ingest_tasks >> complete
