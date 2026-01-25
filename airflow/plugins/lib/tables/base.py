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

    FULL_REFRESH: SELECT * - fetch all records every time
        Use for: Small reference tables, tables without reliable timestamps
        Behavior: Always fetches all records

    INCREMENTAL: Initial full load, then WHERE timestamp > last_sync
        Use for: Large transactional tables with update timestamps
        Requires: pattern_params['timestamp_column']
        Behavior:
            - First sync (no sync state): Full table scan
            - Subsequent syncs: Only new/updated records since last sync

    DATE_PARTITION: WHERE date_col = 'YYYY-MM-DD'
        Use for: Archive tables partitioned by date
        Requires: pattern_params['partition_column']
    """
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"
    DATE_PARTITION = "date_partition"


@dataclass
class TableConfig:
    """Configuration for a table to sync.

    Attributes:
        source_table: Table name in source database (e.g., 'inv_item_mst')
        target_table: Table name in target storage (e.g., 'invoice_item')
        pattern: Ingestion pattern to use
        pattern_params: Parameters specific to the pattern
            - For INCREMENTAL: {'timestamp_column': 'RecordDate'}
            - For DATE_PARTITION: {'partition_column': 'report_date'}
            - For FULL_REFRESH: Optional {'where_clause': 'acct = 123'}
        primary_key: Primary key column(s) for reference
        schema: Source schema name (default: 'dbo')
        description: Human-readable description of the table

    Example:
        TableConfig(
            source_table='inv_item_mst',
            target_table='invoice_item',
            pattern=IngestionPattern.INCREMENTAL,
            pattern_params={'timestamp_column': 'RecordDate'},
            primary_key='item_id',
            description='Invoice line items with prices and quantities'
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
        if self.pattern == IngestionPattern.INCREMENTAL:
            return self.pattern_params.get("timestamp_column")
        return None

    def get_partition_column(self) -> Optional[str]:
        """Get partition column for date partition sync."""
        if self.pattern == IngestionPattern.DATE_PARTITION:
            return self.pattern_params.get("partition_column")
        return None

    def get_where_clause(self) -> Optional[str]:
        """Get optional WHERE clause for full refresh."""
        return self.pattern_params.get("where_clause")

    @property
    def full_source_name(self) -> str:
        """Full qualified source table name: [schema].[table]"""
        return f"[{self.schema}].[{self.source_table}]"

    @property
    def is_incremental(self) -> bool:
        """Check if this table uses incremental sync."""
        return self.pattern == IngestionPattern.INCREMENTAL


def get_tables_by_pattern(
    tables: List[TableConfig],
    pattern: IngestionPattern
) -> List[TableConfig]:
    """Filter tables by ingestion pattern."""
    return [t for t in tables if t.pattern == pattern]


def get_incremental_tables(tables: List[TableConfig]) -> List[TableConfig]:
    """Get tables configured for incremental sync."""
    return get_tables_by_pattern(tables, IngestionPattern.INCREMENTAL)


def get_full_refresh_tables(tables: List[TableConfig]) -> List[TableConfig]:
    """Get tables configured for full refresh."""
    return get_tables_by_pattern(tables, IngestionPattern.FULL_REFRESH)
