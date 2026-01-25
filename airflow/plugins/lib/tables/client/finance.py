"""
Finance-related table configurations.

Tables for general ledger and financial data from client SQL Server.
"""
from lib.tables.base import TableConfig, IngestionPattern


FINANCE_TABLES = [
    TableConfig(
        source_table="ledger_mst",
        target_table="general_ledger",
        pattern=IngestionPattern.FULL_REFRESH,
        pattern_params={
            # Filter to sales rebate account (422200) to reduce data volume
            # while capturing all data needed for rebate calculations
            "where_clause": "acct = '422200'"
        },
        primary_key="trans_num",
        description="General ledger transactions (filtered to sales rebate account)"
    ),
]
