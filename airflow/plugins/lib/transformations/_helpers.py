"""
Shared helper functions for gold layer transformations.

These functions encapsulate reusable business logic that is used
across multiple transformation modules.
"""
import pandas as pd
from datetime import datetime
from typing import Optional

from ._constants import (
    CUSTOMER_ID_MAPPING,
    CUSTOMER_NAME_MAPPING,
    SALES_REBATE_ACCOUNT,
    REBATE_CUSTOMER_ID_MAPPING,
    REBATE_EXCLUDED_PREFIXES,
    REBATE_EXCLUDED_REFS,
)


# =============================================================================
# CUSTOMER NORMALIZATION
# =============================================================================

def normalize_customer_id(cust_num):
    """
    Normalize customer ID using mapping.

    Args:
        cust_num: Raw customer ID from source

    Returns:
        Normalized customer ID (mapped if in CUSTOMER_ID_MAPPING, else original)
    """
    return CUSTOMER_ID_MAPPING.get(cust_num, cust_num)


def normalize_customer_name(name):
    """
    Normalize customer name using mapping.

    Args:
        name: Raw customer name from source

    Returns:
        Normalized customer name (mapped if in CUSTOMER_NAME_MAPPING, else original)
    """
    if pd.isna(name):
        return name
    return CUSTOMER_NAME_MAPPING.get(name, name)


# =============================================================================
# REBATE PROCESSING
# =============================================================================

def get_rebate_cutoff_date() -> datetime:
    """
    Get cutoff date for rebate processing.

    Returns:
        January 1st of last year as the cutoff date
    """
    return datetime(datetime.now().year - 1, 1, 1)


def is_rebate_eligible(acct, trans_date) -> bool:
    """
    Check if a transaction is eligible for rebate extraction.

    A transaction is eligible if:
    - Account matches SALES_REBATE_ACCOUNT
    - Transaction date is on or after the cutoff date

    Args:
        acct: Account code
        trans_date: Transaction date

    Returns:
        True if eligible for rebate extraction
    """
    if pd.isna(acct) or pd.isna(trans_date):
        return False

    if str(acct) != SALES_REBATE_ACCOUNT:
        return False

    try:
        if isinstance(trans_date, str):
            trans_dt = pd.to_datetime(trans_date)
        else:
            trans_dt = trans_date
        return trans_dt >= get_rebate_cutoff_date()
    except (ValueError, TypeError):
        return False


def extract_rebate_customer_name(ref: str) -> str:
    """
    Extract customer name from ref field.

    The customer name is the first word before a space in the ref field.

    Returns empty string if:
    - ref is empty/null
    - ref doesn't contain a space
    - First word is an excluded prefix (Retro, Income)
    - ref is in exclusion list

    Args:
        ref: Reference field value

    Returns:
        Customer name or empty string
    """
    if pd.isna(ref) or not ref:
        return ''

    if ref in REBATE_EXCLUDED_REFS:
        return ''

    if ' ' not in ref:
        return ''

    first_word = ref.split(' ')[0]
    if first_word in REBATE_EXCLUDED_PREFIXES:
        return ''

    return first_word


def get_rebate_customer_id(customer_name: str) -> Optional[int]:
    """
    Map rebate customer name to customer ID.

    Args:
        customer_name: Customer name extracted from ref field

    Returns:
        Customer ID if found in mapping, else None
    """
    if not customer_name:
        return None
    return REBATE_CUSTOMER_ID_MAPPING.get(customer_name)
