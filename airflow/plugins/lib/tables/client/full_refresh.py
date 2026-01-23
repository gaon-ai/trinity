"""
Client SQL Server tables with full refresh sync.

These tables are synced with full table replacement.
Use for small reference tables or tables without reliable timestamps.

Tables:
- custaddr_mst: Customer address master data
- ledger_mst: General ledger master data
- report_lessee: Lessee/customer master data
- report_contract: Contract information
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
        pattern_params={
            # Filter to account 422200 (sales rebate) for rebate calculations
            # This dramatically reduces data volume while getting all needed data
            "where_clause": "acct = '422200'"
        },
        primary_key="trans_num",
        schema="dbo",
        description="General ledger transactions (sales rebate account only)"
    ),
    TableConfig(
        source_table="report_lessee",
        target_table="report_lessee",
        pattern=IngestionPattern.FULL_REFRESH,
        primary_key="lessee_number",
        schema="dbo",
        description="Lessee/customer master data for leasing operations"
    ),
    TableConfig(
        source_table="report_contract",
        target_table="report_contract",
        pattern=IngestionPattern.FULL_REFRESH,
        primary_key="contract_number",
        schema="dbo",
        description="Contract information including start dates"
    ),
]
