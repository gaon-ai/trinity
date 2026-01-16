"""
Client SQL Server tables with full refresh sync.

These tables are synced with full table replacement.
Use for small reference tables or tables without reliable timestamps.

Tables:
- custaddr_mst: Customer address master data
- ledger_mst: General ledger master data
"""
from lib.tables.base import TableConfig, IngestionPattern


FULL_REFRESH_TABLES = [
    TableConfig(
        source_table="custaddr_mst",
        target_table="customer_address",
        pattern=IngestionPattern.FULL_REFRESH,
        primary_key="cust_seq",
        schema="dbo",
        description="Customer address master data"
    ),
    TableConfig(
        source_table="ledger_mst",
        target_table="general_ledger",
        pattern=IngestionPattern.FULL_REFRESH,
        primary_key="acct",
        schema="dbo",
        description="General ledger master data"
    ),
]
