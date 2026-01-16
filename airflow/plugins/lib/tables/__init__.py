"""
Table configuration module for Trinity ETL pipelines.

Provides TableConfig dataclass and IngestionPattern enum for defining
how tables should be synced from source to target.

Usage:
    from lib.tables import TableConfig, IngestionPattern
    from lib.tables.client import INVENTORY_TABLES, INV_ITEM_MST
"""
from lib.tables.base import (
    TableConfig,
    IngestionPattern,
    get_tables_by_pattern,
    get_incremental_tables,
    get_full_refresh_tables,
)

__all__ = [
    'TableConfig',
    'IngestionPattern',
    'get_tables_by_pattern',
    'get_incremental_tables',
    'get_full_refresh_tables',
]
