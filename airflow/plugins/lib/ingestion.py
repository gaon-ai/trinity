"""
Bronze layer ingestion utilities.

Provides reusable functions for ingesting data from source databases
into the bronze layer of the data lake.

Usage:
    from lib.ingestion import ingest_table

    result = ingest_table(
        table_config=my_table_config,
        partition='2025-01-25-14',
        fetch_fn=fetch_query,
        force_full_refresh=False,
    )
"""
import json
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from lib.datalake import write_to_datalake, write_metadata
from lib.sql_templates import render_sql
from lib.sync_metadata import (
    get_last_sync_value,
    get_sync_state,
    update_sync_metadata,
    format_sync_value_for_sql,
)
from lib.tables.base import TableConfig, IngestionPattern


def build_ingestion_sql(
    table_config: TableConfig,
    force_full_refresh: bool = False,
) -> Tuple[str, str]:
    """
    Build SQL query for table ingestion based on pattern and sync state.

    For INCREMENTAL pattern:
    - First sync (no sync state): Returns full refresh SQL
    - Subsequent syncs: Returns incremental SQL with WHERE clause

    Args:
        table_config: Table configuration
        force_full_refresh: Override to force full refresh

    Returns:
        Tuple of (sql_query, effective_mode) where mode is 'full' or 'incremental'
    """
    table_name = table_config.target_table
    pattern = table_config.pattern

    # Force full refresh overrides everything
    if force_full_refresh:
        return _build_full_refresh_sql(table_config), 'full'

    # INCREMENTAL: Check sync state to determine mode
    if pattern == IngestionPattern.INCREMENTAL:
        sync_state = get_sync_state(table_name)

        if sync_state is None:
            # First sync - do full refresh
            return _build_full_refresh_sql(table_config), 'full'
        else:
            # Subsequent sync - do incremental
            return _build_incremental_sql(table_config), 'incremental'

    # DATE_PARTITION: Use partition-based query
    elif pattern == IngestionPattern.DATE_PARTITION:
        # Note: partition_value should be passed in context
        # For now, this is a placeholder
        return _build_date_partition_sql(table_config, None), 'partition'

    # FULL_REFRESH: Always full
    else:
        return _build_full_refresh_sql(table_config), 'full'


def _build_full_refresh_sql(table_config: TableConfig) -> str:
    """Build full refresh SQL query."""
    return render_sql(
        'sqlserver/bronze/full_refresh.sql.j2',
        schema=table_config.schema,
        table=table_config.source_table,
        where_clause=table_config.get_where_clause()
    )


def _build_incremental_sql(table_config: TableConfig) -> str:
    """Build incremental SQL query with timestamp filter."""
    last_value = get_last_sync_value(table_config.target_table)
    last_value_str = format_sync_value_for_sql(last_value)

    return render_sql(
        'sqlserver/bronze/incremental.sql.j2',
        schema=table_config.schema,
        table=table_config.source_table,
        timestamp_column=table_config.pattern_params['timestamp_column'],
        last_sync_value=last_value_str
    )


def _build_date_partition_sql(table_config: TableConfig, partition_value: str) -> str:
    """Build date partition SQL query."""
    return render_sql(
        'sqlserver/bronze/date_partition.sql.j2',
        schema=table_config.schema,
        table=table_config.source_table,
        partition_column=table_config.pattern_params['partition_column'],
        partition_value=partition_value
    )


def ingest_table(
    table_config: TableConfig,
    partition: str,
    fetch_fn: Callable[[str], List[Dict[str, Any]]],
    force_full_refresh: bool = False,
    source_name: str = 'sqlserver',
) -> Dict[str, Any]:
    """
    Ingest a single table from source to bronze layer.

    This is the main entry point for table ingestion. It handles:
    - Building the appropriate SQL based on pattern and sync state
    - Fetching data from source
    - Writing to data lake
    - Updating sync metadata

    Args:
        table_config: TableConfig defining the table
        partition: Partition string (YYYY-MM-DD-HH)
        fetch_fn: Function to execute SQL and return rows
        force_full_refresh: Override to force full refresh
        source_name: Source identifier for metadata

    Returns:
        Dict with ingestion results:
        - table: Target table name
        - rows_synced: Number of rows written
        - path: Data lake path (if data written)
        - mode: 'full' or 'incremental'
        - duration_seconds: Time taken
        - skipped: True if no data to sync
    """
    table_name = table_config.target_table
    start_time = time.time()

    print(f"Starting ingestion for {table_config.full_source_name}")
    print(f"  Pattern: {table_config.pattern.value}")

    # Build SQL query
    sql, mode = build_ingestion_sql(table_config, force_full_refresh)

    if force_full_refresh:
        print(f"  Mode: FULL REFRESH (forced)")
    elif mode == 'full' and table_config.pattern == IngestionPattern.INCREMENTAL:
        print(f"  Mode: INITIAL FULL (no sync state)")
    else:
        print(f"  Mode: {mode.upper()}")

    if mode == 'incremental':
        last_value = get_last_sync_value(table_name)
        print(f"  Last sync value: {format_sync_value_for_sql(last_value)}")

    # Execute query
    print(f"  Executing query...")
    rows = fetch_fn(sql)
    row_count = len(rows)
    print(f"  Fetched {row_count} rows")

    duration = time.time() - start_time

    # No data case
    if row_count == 0:
        print(f"  No new data to sync")
        return {
            'table': table_name,
            'rows_synced': 0,
            'mode': mode,
            'duration_seconds': round(duration, 2),
            'skipped': True,
        }

    # Write to data lake
    path = f"client/{table_name}/{partition}/data.json"
    json_data = json.dumps(rows, indent=2, default=str)
    write_to_datalake('bronze', path, json_data)

    # Write lineage metadata
    write_metadata('bronze', path, extra={
        'source': source_name,
        'source_table': table_config.full_source_name,
        'dataset': table_name,
        'partition': partition,
        'record_count': row_count,
        'pattern': table_config.pattern.value,
        'mode': mode,
    })

    # Update sync state for incremental tables
    if table_config.pattern == IngestionPattern.INCREMENTAL:
        _update_incremental_sync_state(table_config, rows, row_count, duration, partition, path)

    print(f"  Ingestion complete: {row_count} records → bronze/{path}")
    print(f"  Duration: {duration:.2f}s")

    return {
        'table': table_name,
        'path': path,
        'rows_synced': row_count,
        'mode': mode,
        'duration_seconds': round(duration, 2),
    }


def _update_incremental_sync_state(
    table_config: TableConfig,
    rows: List[Dict[str, Any]],
    row_count: int,
    duration: float,
    partition: str,
    path: str,
) -> None:
    """Update sync metadata after successful incremental ingestion."""
    ts_column = table_config.pattern_params['timestamp_column']

    # Find max timestamp from synced rows
    max_ts_values = [r[ts_column] for r in rows if r.get(ts_column)]
    if not max_ts_values:
        return

    max_ts = max(max_ts_values)
    if isinstance(max_ts, str):
        max_ts = datetime.fromisoformat(max_ts)

    update_sync_metadata(
        table_name=table_config.target_table,
        last_sync_value=max_ts,
        rows_synced=row_count,
        sync_type=table_config.pattern.value,
        duration_seconds=duration,
        extra={
            'partition': partition,
            'path': f"bronze/{path}"
        }
    )
