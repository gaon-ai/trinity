"""
Client SQL Server tables with timestamp-based incremental sync.

These tables are synced daily using a timestamp column to track changes.
Only new/updated records since last sync are fetched.

Tables:
- inv_item_mst: Inventory item master data
- coitem_mst: Customer order item details
- inv_hdr_mst: Inventory header/transaction data
"""
from lib.tables.base import TableConfig, IngestionPattern


INCREMENTAL_TABLES = [
    TableConfig(
        source_table="inv_item_mst",
        target_table="inv_item_mst",
        pattern=IngestionPattern.TIMESTAMP_INCREMENTAL,
        pattern_params={"timestamp_column": "lastupdated"},
        primary_key="item_id",
        schema="dbo",
        description="Inventory item master data - products and SKUs"
    ),
    TableConfig(
        source_table="coitem_mst",
        target_table="coitem_mst",
        pattern=IngestionPattern.TIMESTAMP_INCREMENTAL,
        pattern_params={"timestamp_column": "lastupdated"},
        primary_key="co_num",
        schema="dbo",
        description="Customer order item details"
    ),
    TableConfig(
        source_table="inv_hdr_mst",
        target_table="inv_hdr_mst",
        pattern=IngestionPattern.TIMESTAMP_INCREMENTAL,
        pattern_params={"timestamp_column": "lastupdated"},
        primary_key="inv_num",
        schema="dbo",
        description="Inventory header/transaction data"
    ),
]
