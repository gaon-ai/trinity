"""
Base table configuration classes for Trinity ETL pipelines.

Provides:
- IngestionPattern: Enum defining sync strategies
- TableConfig: Dataclass for table sync configuration
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any, List


class IngestionPattern(Enum):
    """Defines how a table's data should be queried for ingestion.

    FULL_REFRESH: SELECT * - fetch all records, replace target data
        Use for: Small reference tables, tables without reliable timestamps

    TIMESTAMP_INCREMENTAL: WHERE timestamp_col > last_sync_value
        Use for: Large transactional tables with update timestamps
        Requires: pattern_params['timestamp_column']

    DATE_PARTITION: WHERE date_col = 'YYYY-MM-DD'
        Use for: Archive tables partitioned by date
        Requires: pattern_params['partition_column']
    """
    FULL_REFRESH = "full_refresh"
    TIMESTAMP_INCREMENTAL = "timestamp"
    DATE_PARTITION = "date_partition"


@dataclass
class TableConfig:
    """Configuration for a table to sync.

    Attributes:
        source_table: Table name in source database (e.g., 'inv_item_mst')
        target_table: Table name in target storage (e.g., 'inv_item_mst')
        pattern: Ingestion pattern to use
        pattern_params: Parameters specific to the pattern
            - For TIMESTAMP_INCREMENTAL: {'timestamp_column': 'lastupdated'}
            - For DATE_PARTITION: {'partition_column': 'report_date'}
        primary_key: Primary key column(s) for upsert operations
        schema: Source schema name (default: 'dbo')
        description: Human-readable description of the table

    Example:
        TableConfig(
            source_table='inv_item_mst',
            target_table='inv_item_mst',
            pattern=IngestionPattern.TIMESTAMP_INCREMENTAL,
            pattern_params={'timestamp_column': 'lastupdated'},
            primary_key='item_id',
            schema='dbo',
            description='Inventory item master data'
        )
    """
    source_table: str
    target_table: str
    pattern: IngestionPattern = IngestionPattern.FULL_REFRESH
    pattern_params: Dict[str, Any] = field(default_factory=dict)
    primary_key: Optional[str] = None
    schema: str = "dbo"
    description: Optional[str] = None

    def get_timestamp_column(self) -> Optional[str]:
        """Get timestamp column for incremental sync."""
        if self.pattern == IngestionPattern.TIMESTAMP_INCREMENTAL:
            return self.pattern_params.get("timestamp_column")
        return None

    def get_partition_column(self) -> Optional[str]:
        """Get partition column for date partition sync."""
        if self.pattern == IngestionPattern.DATE_PARTITION:
            return self.pattern_params.get("partition_column")
        return None

    @property
    def full_source_name(self) -> str:
        """Full qualified source table name: [schema].[table]"""
        return f"[{self.schema}].[{self.source_table}]"


def get_tables_by_pattern(
    tables: List[TableConfig],
    pattern: IngestionPattern
) -> List[TableConfig]:
    """Filter tables by ingestion pattern.

    Args:
        tables: List of TableConfig objects
        pattern: The pattern to filter by

    Returns:
        List of tables matching the pattern
    """
    return [t for t in tables if t.pattern == pattern]


def get_incremental_tables(tables: List[TableConfig]) -> List[TableConfig]:
    """Get tables configured for incremental sync."""
    return get_tables_by_pattern(tables, IngestionPattern.TIMESTAMP_INCREMENTAL)


def get_full_refresh_tables(tables: List[TableConfig]) -> List[TableConfig]:
    """Get tables configured for full refresh."""
    return get_tables_by_pattern(tables, IngestionPattern.FULL_REFRESH)
