"""
General ledger fact table transformations.

Source tables:
- general_ledger (ledger_mst) -> fact_general_ledger, margin_rebate
"""
import pandas as pd

from lib.transformations._constants import SALES_REBATE_ACCOUNT
from lib.transformations._helpers import extract_rebate_columns, safe_to_datetime

__all__ = ['transform_fact_general_ledger', 'create_margin_rebate']


def transform_fact_general_ledger(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform general_ledger -> fact_general_ledger.

    Source: bronze/client/general_ledger (from ledger_mst)
    Primary Key: trans_num

    Extracts rebate customer information from ref field for sales rebate account.

    Columns:
        Account: Account code
        Transaction_Date: Transaction date
        Domestic_Amount: Domestic currency amount
        Foreign_Amount: Foreign currency amount
        FromID: Source identifier
        Reference: Reference field
        RebateCustomerName: Extracted customer name (for rebate account only)
        RebateCustomerID: Mapped customer ID (for rebate account only)
    """
    # Use vectorized rebate extraction for performance
    rebate_info = extract_rebate_columns(df)

    return pd.DataFrame({
        'Account': df['acct'],
        'Transaction_Date': safe_to_datetime(df['trans_date']),
        'Domestic_Amount': pd.to_numeric(df['dom_amount'], errors='coerce').fillna(0),
        'Foreign_Amount': pd.to_numeric(df['for_amount'], errors='coerce').fillna(0),
        'FromID': df['from_id'],
        'Reference': df['ref'],
        'RebateCustomerName': rebate_info['RebateCustomerName'],
        'RebateCustomerID': rebate_info['RebateCustomerID'],
    })


def create_margin_rebate(fact_general_ledger: pd.DataFrame) -> pd.DataFrame:
    """
    Create margin_rebate by aggregating rebates from fact_general_ledger.

    Filters to sales rebate account with valid customer names,
    then groups by customer to sum amounts.

    Args:
        fact_general_ledger: Output of transform_fact_general_ledger()

    Returns:
        DataFrame with columns: RebateCustomerName, RebateCustomerID, Domestic_Amount
    """
    filtered = fact_general_ledger[
        (fact_general_ledger['Account'].astype(str) == SALES_REBATE_ACCOUNT) &
        (fact_general_ledger['RebateCustomerName'] != '') &
        (fact_general_ledger['RebateCustomerName'].notna())
    ]

    if len(filtered) == 0:
        return pd.DataFrame(columns=['RebateCustomerName', 'RebateCustomerID', 'Domestic_Amount'])

    return filtered.groupby(
        ['RebateCustomerName', 'RebateCustomerID'],
        as_index=False
    ).agg({'Domestic_Amount': 'sum'})
