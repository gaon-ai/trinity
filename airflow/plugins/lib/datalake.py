"""
Azure Data Lake Storage Operations

Common functions for reading/writing to Azure Data Lake Gen2.
"""
import os
import json
import time
from datetime import datetime
from typing import Any, Optional

from azure.storage.filedatalake import DataLakeServiceClient


# Configuration from environment
STORAGE_ACCOUNT_NAME = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME', '')
STORAGE_ACCOUNT_KEY = os.environ.get('AZURE_STORAGE_ACCOUNT_KEY', '')


def get_datalake_client() -> DataLakeServiceClient:
    """Create Data Lake service client."""
    if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
        raise ValueError(
            "Missing AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT_KEY. "
            "Set these in /opt/airflow/.env"
        )
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)


def write_to_datalake(container: str, path: str, data: str) -> None:
    """
    Write data to Data Lake.

    Args:
        container: Container name (bronze, silver, gold)
        path: File path within container
        data: String data to write
    """
    service_client = get_datalake_client()
    file_system_client = service_client.get_file_system_client(container)
    file_client = file_system_client.get_file_client(path)
    file_client.upload_data(data, overwrite=True)
    print(f"Wrote data to {container}/{path}")


def read_from_datalake(container: str, path: str) -> str:
    """
    Read data from Data Lake.

    Args:
        container: Container name (bronze, silver, gold)
        path: File path within container

    Returns:
        File contents as string
    """
    service_client = get_datalake_client()
    file_system_client = service_client.get_file_system_client(container)
    file_client = file_system_client.get_file_client(path)
    download = file_client.download_file()
    return download.readall().decode('utf-8')


def file_exists(container: str, path: str) -> bool:
    """
    Check if file exists in Data Lake.

    Args:
        container: Container name
        path: File path within container

    Returns:
        True if file exists, False otherwise
    """
    try:
        service_client = get_datalake_client()
        file_system_client = service_client.get_file_system_client(container)
        file_client = file_system_client.get_file_client(path)
        file_client.get_file_properties()
        return True
    except Exception:
        return False


def wait_for_file(
    container: str,
    path: str,
    timeout_seconds: int = 300,
    poll_interval: int = 10,
    raise_on_timeout: bool = True
) -> bool:
    """
    Wait for a file to exist in Data Lake with retry logic.

    Use this to wait for upstream data before processing.

    Args:
        container: Container name (bronze, silver, gold)
        path: File path within container
        timeout_seconds: Maximum time to wait (default: 5 minutes)
        poll_interval: Seconds between checks (default: 10)
        raise_on_timeout: If True, raise exception on timeout; else return False

    Returns:
        True if file exists within timeout

    Raises:
        TimeoutError: If file not found within timeout (when raise_on_timeout=True)

    Example:
        >>> wait_for_file('bronze', 'erp/sales/2025-12-30-14/raw.json')
        True
    """
    full_path = f"{container}/{path}"
    print(f"Waiting for {full_path} (timeout: {timeout_seconds}s)...")

    start_time = time.time()
    attempts = 0

    while time.time() - start_time < timeout_seconds:
        attempts += 1
        if file_exists(container, path):
            elapsed = time.time() - start_time
            print(f"Found {full_path} after {elapsed:.1f}s ({attempts} attempts)")
            return True

        print(f"  Attempt {attempts}: Not found, retrying in {poll_interval}s...")
        time.sleep(poll_interval)

    elapsed = time.time() - start_time
    msg = f"Timeout waiting for {full_path} after {elapsed:.1f}s ({attempts} attempts)"

    if raise_on_timeout:
        raise TimeoutError(msg)

    print(f"Warning: {msg}")
    return False


def wait_for_files(
    files: list[tuple[str, str]],
    timeout_seconds: int = 300,
    poll_interval: int = 10,
    require_all: bool = True
) -> dict[str, bool]:
    """
    Wait for multiple files to exist.

    Args:
        files: List of (container, path) tuples
        timeout_seconds: Maximum time to wait for all files
        poll_interval: Seconds between checks
        require_all: If True, raise if any file missing; else return status dict

    Returns:
        Dict mapping full paths to existence status

    Example:
        >>> wait_for_files([
        ...     ('bronze', 'erp/sales/2025-12-30-14/raw.json'),
        ...     ('bronze', 'erp/orders/2025-12-30-14/raw.json'),
        ... ])
        {'bronze/erp/sales/...': True, 'bronze/erp/orders/...': True}
    """
    print(f"Waiting for {len(files)} files (timeout: {timeout_seconds}s)...")

    start_time = time.time()
    results = {f"{c}/{p}": False for c, p in files}

    while time.time() - start_time < timeout_seconds:
        all_found = True
        for container, path in files:
            full_path = f"{container}/{path}"
            if not results[full_path]:
                if file_exists(container, path):
                    results[full_path] = True
                    print(f"  Found: {full_path}")
                else:
                    all_found = False

        if all_found:
            elapsed = time.time() - start_time
            print(f"All files found after {elapsed:.1f}s")
            return results

        time.sleep(poll_interval)

    missing = [p for p, found in results.items() if not found]
    msg = f"Timeout: {len(missing)} file(s) not found: {missing}"

    if require_all:
        raise TimeoutError(msg)

    print(f"Warning: {msg}")
    return results


def write_metadata(
    container: str,
    path: str,
    source_layer: str = None,
    source_path: str = None,
    extra: dict = None
) -> None:
    """
    Write metadata file for lineage tracking.

    Args:
        container: Container name
        path: Directory path (metadata written as _metadata.json)
        source_layer: Source layer name (bronze, silver, gold)
        source_path: Full source path
        extra: Additional metadata fields
    """
    metadata = {
        'target_layer': container,
        'target_path': f"{container}/{path}",
        'written_at': datetime.now().isoformat(),
    }

    if source_layer:
        metadata['source_layer'] = source_layer
    if source_path:
        metadata['source_path'] = source_path
    if extra:
        metadata.update(extra)

    # Write to _metadata.json in same directory
    dir_path = '/'.join(path.split('/')[:-1])
    metadata_path = f"{dir_path}/_metadata.json"
    write_to_datalake(container, metadata_path, json.dumps(metadata, indent=2))
