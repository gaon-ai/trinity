"""
Verification Utilities for Trinity ETL Pipelines.

Provides data quality checks and operational monitoring for
bronze/silver/gold layer data.

Usage:
    from lib.verification import (
        verify_bronze_table,
        get_sync_summary,
        print_verification_report,
    )
"""
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from lib.datalake import file_exists, read_from_datalake, get_datalake_client
from lib.sync_metadata import get_all_sync_states, get_sync_state


def verify_bronze_table(
    table_name: str,
    partition: str
) -> Dict[str, Any]:
    """
    Verify bronze layer data exists and is valid.

    Args:
        table_name: Target table name
        partition: Partition path (e.g., '2025-01-15-14')

    Returns:
        Dict with verification results
    """
    path = f"client/{table_name}/{partition}/data.json"
    result = {
        'table': table_name,
        'partition': partition,
        'path': f"bronze/{path}",
        'exists': False,
        'record_count': 0,
        'verified_at': datetime.utcnow().isoformat() + "Z",
    }

    if not file_exists('bronze', path):
        result['status'] = 'MISSING'
        result['error'] = f"File not found: bronze/{path}"
        return result

    try:
        data = json.loads(read_from_datalake('bronze', path))
        result['exists'] = True
        result['record_count'] = len(data) if isinstance(data, list) else 1
        result['status'] = 'OK' if result['record_count'] > 0 else 'EMPTY'

        # Check metadata file
        metadata_path = f"client/{table_name}/{partition}/_metadata.json"
        if file_exists('bronze', metadata_path):
            metadata = json.loads(read_from_datalake('bronze', metadata_path))
            result['metadata'] = metadata
        else:
            result['metadata_missing'] = True

    except Exception as e:
        result['status'] = 'ERROR'
        result['error'] = str(e)

    return result


def get_sync_summary() -> Dict[str, Any]:
    """
    Get summary of all sync states.

    Returns:
        Dict with sync summary statistics
    """
    states = get_all_sync_states()

    if not states:
        return {
            'total_tables': 0,
            'message': 'No sync states found'
        }

    total_rows = sum(s.get('total_rows_synced', 0) for s in states)
    last_syncs = [
        datetime.fromisoformat(s['last_sync_at'].rstrip('Z'))
        for s in states if s.get('last_sync_at')
    ]

    return {
        'total_tables': len(states),
        'total_rows_synced': total_rows,
        'oldest_sync': min(last_syncs).isoformat() if last_syncs else None,
        'newest_sync': max(last_syncs).isoformat() if last_syncs else None,
        'tables': [
            {
                'table': s['table_name'],
                'last_sync': s.get('last_sync_at'),
                'rows': s.get('rows_synced', 0),
                'type': s.get('sync_type'),
            }
            for s in states
        ]
    }


def check_stale_syncs(
    max_age_hours: int = 24
) -> List[Dict[str, Any]]:
    """
    Find tables that haven't been synced recently.

    Args:
        max_age_hours: Maximum acceptable hours since last sync

    Returns:
        List of stale table info
    """
    states = get_all_sync_states()
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    stale = []

    for state in states:
        last_sync_str = state.get('last_sync_at')
        if not last_sync_str:
            stale.append({
                'table': state['table_name'],
                'last_sync': None,
                'reason': 'Never synced'
            })
            continue

        last_sync = datetime.fromisoformat(last_sync_str.rstrip('Z'))
        if last_sync < cutoff:
            hours_ago = (datetime.utcnow() - last_sync).total_seconds() / 3600
            stale.append({
                'table': state['table_name'],
                'last_sync': last_sync_str,
                'hours_ago': round(hours_ago, 1),
                'reason': f'Sync older than {max_age_hours}h'
            })

    return stale


def list_bronze_partitions(
    table_name: str,
    limit: int = 10
) -> List[str]:
    """
    List available partitions for a bronze table.

    Args:
        table_name: Target table name
        limit: Maximum partitions to return

    Returns:
        List of partition paths (newest first)
    """
    service_client = get_datalake_client()
    file_system_client = service_client.get_file_system_client('bronze')

    partitions = []
    prefix = f"client/{table_name}/"

    try:
        paths = file_system_client.get_paths(path=prefix)
        for path_item in paths:
            # Extract partition from path like client/inv_item_mst/2025-01-15-14/
            if path_item.is_directory:
                parts = path_item.name.split('/')
                if len(parts) >= 3:
                    partitions.append(parts[2])

        # Sort descending (newest first) and limit
        partitions = sorted(set(partitions), reverse=True)[:limit]

    except Exception as e:
        print(f"Warning: Could not list partitions: {e}")

    return partitions


def print_verification_report(table_name: Optional[str] = None) -> None:
    """
    Print a human-readable verification report.

    Args:
        table_name: Optional table to verify (all tables if None)
    """
    print("=" * 60)
    print("TRINITY ETL VERIFICATION REPORT")
    print(f"Generated: {datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    # Sync summary
    summary = get_sync_summary()
    print(f"\nSYNC SUMMARY")
    print(f"  Total tables tracked: {summary.get('total_tables', 0)}")
    print(f"  Total rows synced: {summary.get('total_rows_synced', 0):,}")
    print(f"  Oldest sync: {summary.get('oldest_sync', 'N/A')}")
    print(f"  Newest sync: {summary.get('newest_sync', 'N/A')}")

    # Stale syncs
    stale = check_stale_syncs(max_age_hours=24)
    if stale:
        print(f"\nSTALE TABLES (>{24}h since sync):")
        for s in stale:
            print(f"  - {s['table']}: {s['reason']}")
    else:
        print(f"\nNo stale tables (all synced within 24h)")

    # Table details
    if table_name:
        print(f"\nTABLE DETAILS: {table_name}")
        state = get_sync_state(table_name)
        if state:
            print(f"  Last sync: {state.get('last_sync_at', 'N/A')}")
            print(f"  Last value: {state.get('last_sync_value', 'N/A')}")
            print(f"  Rows synced: {state.get('rows_synced', 0)}")
            print(f"  Total rows: {state.get('total_rows_synced', 0):,}")
            print(f"  Sync type: {state.get('sync_type', 'N/A')}")
        else:
            print(f"  No sync state found for {table_name}")

        partitions = list_bronze_partitions(table_name, limit=5)
        if partitions:
            print(f"\n  Recent partitions:")
            for p in partitions:
                print(f"    - {p}")

    print("\n" + "=" * 60)
