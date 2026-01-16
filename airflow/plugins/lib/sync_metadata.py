"""
Sync Metadata Tracking for Incremental Loads.

Tracks last sync state for tables using incremental ingestion patterns.
State is stored as JSON files in Azure Data Lake: bronze/_sync_state/{table_name}.json

Example sync state structure:
{
    "table_name": "inv_item_mst",
    "last_sync_value": "2025-01-15T10:30:00",
    "last_sync_at": "2025-01-15T14:00:00Z",
    "rows_synced": 1250,
    "sync_type": "timestamp_incremental",
    "sync_duration_seconds": 45.2
}
"""
import json
from datetime import datetime
from typing import Optional, List, Dict, Any

from lib.datalake import read_from_datalake, write_to_datalake, file_exists


# Location for sync state files
SYNC_STATE_CONTAINER = "bronze"
SYNC_STATE_PREFIX = "_sync_state"


def _get_sync_state_path(table_name: str) -> str:
    """Get the Data Lake path for a table's sync state."""
    return f"{SYNC_STATE_PREFIX}/{table_name}.json"


def get_last_sync_value(table_name: str) -> Optional[datetime]:
    """
    Get the last sync timestamp for a table.

    Used to build incremental WHERE clause:
        WHERE timestamp_col > '{last_sync_value}'

    Args:
        table_name: Target table name (e.g., 'inv_item_mst')

    Returns:
        datetime of last sync, or None if never synced

    Example:
        >>> last = get_last_sync_value('inv_item_mst')
        >>> if last:
        ...     sql = f"SELECT * FROM table WHERE updated > '{last}'"
    """
    path = _get_sync_state_path(table_name)

    if not file_exists(SYNC_STATE_CONTAINER, path):
        return None

    try:
        data = json.loads(read_from_datalake(SYNC_STATE_CONTAINER, path))
        last_value = data.get("last_sync_value")
        if last_value:
            return datetime.fromisoformat(last_value)
        return None
    except Exception as e:
        print(f"Warning: Could not read sync state for {table_name}: {e}")
        return None


def get_sync_state(table_name: str) -> Optional[Dict[str, Any]]:
    """
    Get full sync state for a table.

    Args:
        table_name: Target table name

    Returns:
        Full sync state dict, or None if not found
    """
    path = _get_sync_state_path(table_name)

    if not file_exists(SYNC_STATE_CONTAINER, path):
        return None

    try:
        return json.loads(read_from_datalake(SYNC_STATE_CONTAINER, path))
    except Exception as e:
        print(f"Warning: Could not read sync state for {table_name}: {e}")
        return None


def update_sync_metadata(
    table_name: str,
    last_sync_value: datetime,
    rows_synced: int,
    sync_type: str,
    duration_seconds: float,
    extra: Optional[Dict[str, Any]] = None
) -> None:
    """
    Update sync state after successful sync.

    Args:
        table_name: Target table name
        last_sync_value: Maximum timestamp value from synced records
        rows_synced: Number of rows synced in this batch
        sync_type: Ingestion pattern used (e.g., 'timestamp_incremental')
        duration_seconds: Time taken for sync operation
        extra: Additional metadata to store

    Example:
        >>> update_sync_metadata(
        ...     table_name='inv_item_mst',
        ...     last_sync_value=datetime(2025, 1, 15, 10, 30),
        ...     rows_synced=1250,
        ...     sync_type='timestamp_incremental',
        ...     duration_seconds=45.2
        ... )
    """
    path = _get_sync_state_path(table_name)

    # Build sync state
    state = {
        "table_name": table_name,
        "last_sync_value": last_sync_value.isoformat(),
        "last_sync_at": datetime.utcnow().isoformat() + "Z",
        "rows_synced": rows_synced,
        "sync_type": sync_type,
        "sync_duration_seconds": round(duration_seconds, 2),
    }

    # Add extra metadata if provided
    if extra:
        state["extra"] = extra

    # Load previous state to track history
    previous_state = get_sync_state(table_name)
    if previous_state:
        state["previous_sync_at"] = previous_state.get("last_sync_at")
        state["previous_sync_value"] = previous_state.get("last_sync_value")
        # Accumulate total rows synced
        total_rows = previous_state.get("total_rows_synced", 0) + rows_synced
        state["total_rows_synced"] = total_rows
    else:
        state["total_rows_synced"] = rows_synced

    write_to_datalake(SYNC_STATE_CONTAINER, path, json.dumps(state, indent=2))
    print(f"Updated sync state for {table_name}: {rows_synced} rows, "
          f"last_value={last_sync_value.isoformat()}")


def get_all_sync_states() -> List[Dict[str, Any]]:
    """
    Get sync state for all tracked tables.

    Returns:
        List of sync state dicts for all tables

    Note:
        This requires listing files in the _sync_state directory.
        For large numbers of tables, consider caching results.
    """
    from azure.storage.filedatalake import DataLakeServiceClient
    from lib.datalake import get_datalake_client

    states = []

    try:
        service_client = get_datalake_client()
        file_system_client = service_client.get_file_system_client(SYNC_STATE_CONTAINER)

        # List all files in sync state directory
        paths = file_system_client.get_paths(path=SYNC_STATE_PREFIX)

        for path_item in paths:
            if path_item.name.endswith('.json'):
                try:
                    state = json.loads(
                        read_from_datalake(SYNC_STATE_CONTAINER, path_item.name)
                    )
                    states.append(state)
                except Exception as e:
                    print(f"Warning: Could not read {path_item.name}: {e}")

    except Exception as e:
        print(f"Warning: Could not list sync states: {e}")

    return states


def format_sync_value_for_sql(value: Optional[datetime], default: str = "1900-01-01") -> str:
    """
    Format a datetime value for use in SQL WHERE clause.

    Args:
        value: datetime value (or None)
        default: Default value if None (far past date)

    Returns:
        Formatted string for SQL (e.g., '2025-01-15 10:30:00')
    """
    if value is None:
        return default
    return value.strftime("%Y-%m-%d %H:%M:%S")


def reset_sync_state(table_name: str) -> bool:
    """
    Reset sync state for a table (force full refresh on next sync).

    Args:
        table_name: Target table name

    Returns:
        True if state was reset, False if no state existed
    """
    path = _get_sync_state_path(table_name)

    if not file_exists(SYNC_STATE_CONTAINER, path):
        print(f"No sync state found for {table_name}")
        return False

    # Archive the old state before resetting
    old_state = get_sync_state(table_name)
    reset_state = {
        "table_name": table_name,
        "reset_at": datetime.utcnow().isoformat() + "Z",
        "previous_state": old_state,
        "status": "reset"
    }

    write_to_datalake(SYNC_STATE_CONTAINER, path, json.dumps(reset_state, indent=2))
    print(f"Reset sync state for {table_name}")
    return True
