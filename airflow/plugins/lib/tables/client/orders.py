"""
Order-related table configurations.

Tables for customer order data from client SQL Server.
"""
from lib.tables.base import TableConfig, IngestionPattern


ORDER_TABLES = [
    TableConfig(
        source_table="coitem_mst",
        target_table="order_item",
        pattern=IngestionPattern.INCREMENTAL,
        pattern_params={"timestamp_column": "RecordDate"},
        primary_key="co_num",
        description="Customer order line items with quantities and pricing"
    ),
]
