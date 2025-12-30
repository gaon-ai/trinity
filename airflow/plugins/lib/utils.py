"""
Utility Functions

Common helpers for DAGs.
"""
from datetime import datetime


def get_date_hour(context) -> str:
    """
    Get execution date in YYYY-MM-DD-HH format from Airflow context.

    This is the standard partition format used across all layers.

    Args:
        context: Airflow task context

    Returns:
        Date string in YYYY-MM-DD-HH format (e.g., '2025-12-30-14')
    """
    dt = context.get('logical_date') or context.get('execution_date')
    return dt.strftime('%Y-%m-%d-%H')


def get_execution_date(context) -> str:
    """
    Get execution date in YYYY-MM-DD format from Airflow context.

    Used for report_date fields in Gold layer.

    Args:
        context: Airflow task context

    Returns:
        Date string in YYYY-MM-DD format (e.g., '2025-12-30')
    """
    return context['ds']


def build_bronze_path(source: str, dataset: str, partition: str, filename: str) -> str:
    """
    Build Bronze layer path.

    Pattern: {source}/{dataset}/{YYYY-MM-DD-HH}/{filename}

    Args:
        source: Data source (e.g., 'erp')
        dataset: Dataset name (e.g., 'sales')
        partition: Partition key (YYYY-MM-DD-HH)
        filename: File name (e.g., 'raw.json')

    Returns:
        Full path within bronze container
    """
    return f"{source}/{dataset}/{partition}/{filename}"


def build_silver_path(dataset: str, partition: str, filename: str) -> str:
    """
    Build Silver layer path.

    Pattern: {dataset}/{YYYY-MM-DD-HH}/{filename}

    Args:
        dataset: Dataset name (e.g., 'sales')
        partition: Partition key (YYYY-MM-DD-HH)
        filename: File name (e.g., 'cleaned.csv')

    Returns:
        Full path within silver container
    """
    return f"{dataset}/{partition}/{filename}"


def build_gold_path(dataset: str, partition: str, filename: str) -> str:
    """
    Build Gold layer path.

    Pattern: {dataset}/{YYYY-MM-DD-HH}/{filename}

    Args:
        dataset: Dataset name (e.g., 'sales_by_region')
        partition: Partition key (YYYY-MM-DD-HH)
        filename: File name (e.g., 'data.csv')

    Returns:
        Full path within gold container
    """
    return f"{dataset}/{partition}/{filename}"
