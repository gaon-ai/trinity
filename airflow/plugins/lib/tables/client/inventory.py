"""
Inventory-related table configurations.

Tables for invoice and inventory data from client SQL Server.
"""
from lib.tables.base import TableConfig, IngestionPattern


INVENTORY_TABLES = [
    TableConfig(
        source_table="inv_item_mst",
        target_table="invoice_item",
        pattern=IngestionPattern.INCREMENTAL,
        pattern_params={"timestamp_column": "RecordDate"},
        primary_key="item_id",
        description="Invoice line items with prices, quantities, and costs"
    ),
    TableConfig(
        source_table="inv_hdr_mst",
        target_table="invoice_header",
        pattern=IngestionPattern.INCREMENTAL,
        pattern_params={"timestamp_column": "RecordDate"},
        primary_key="inv_num",
        description="Invoice headers with customer linkage"
    ),
]
