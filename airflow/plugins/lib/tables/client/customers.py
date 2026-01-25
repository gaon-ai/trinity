"""
Customer-related table configurations.

Tables for customer master data from client SQL Server.
"""
from lib.tables.base import TableConfig, IngestionPattern


CUSTOMER_TABLES = [
    TableConfig(
        source_table="custaddr_mst",
        target_table="customer_address",
        pattern=IngestionPattern.FULL_REFRESH,
        primary_key="cust_seq",
        description="Customer addresses and contact information"
    ),
]
