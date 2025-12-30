"""
Azure Data Lake Storage Operations

Common functions for reading/writing to Azure Data Lake Gen2.
"""
import os
import json
from datetime import datetime
from typing import Any

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
