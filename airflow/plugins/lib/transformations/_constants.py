"""
Shared business constants for gold layer transformations.

This module contains mappings, account codes, and other business rules
that are shared across multiple transformation modules.

To extend:
- Add new customer mappings to CUSTOMER_ID_MAPPING or CUSTOMER_NAME_MAPPING
- Add new rebate customers to REBATE_CUSTOMER_ID_MAPPING
- Add exclusion rules to REBATE_EXCLUDED_PREFIXES or REBATE_EXCLUDED_REFS
"""

__all__ = [
    'SALES_REBATE_ACCOUNT',
    'CUSTOMER_ID_MAPPING',
    'CUSTOMER_NAME_MAPPING',
    'REBATE_CUSTOMER_ID_MAPPING',
    'REBATE_EXCLUDED_PREFIXES',
    'REBATE_EXCLUDED_REFS',
]

# =============================================================================
# ACCOUNT CODES
# =============================================================================

SALES_REBATE_ACCOUNT = '422200'


# =============================================================================
# CUSTOMER MAPPINGS
# =============================================================================

# Customer ID normalization - consolidate related customer IDs
# Format: {source_id: normalized_id}
CUSTOMER_ID_MAPPING = {
    1064: 1015,  # CVS/Omnicare -> CVS
    1314: 1005,  # Owens variant -> Owens
}

# Customer name normalization - standardize customer names
# Format: {source_name: normalized_name}
CUSTOMER_NAME_MAPPING = {
    'CVS CAREMARK': 'CVS/Omnicare',
    'OMNICARE': 'CVS/Omnicare',
    'Kaleida Health': 'Olean General',
    'METHODIST HOSPITAL WESTOVER HILLS / San Antonio Supply Chain': 'Methodist Hospital Westover Hills',
    'Methodist Le Bonheur Healthcare': 'Methodist Healthcare',
    "O&M Main Street": 'Owens',
    'Owens & Minor': 'Owens',
}


# =============================================================================
# REBATE CONFIGURATION
# =============================================================================

# Rebate customer name to ID mapping (for sales rebate account)
# Format: {customer_name_from_ref: customer_id}
REBATE_CUSTOMER_ID_MAPPING = {
    'Mckesson': 1004,
    'Cardinal': 1008,
    'Schein': 1030,
    'Medline': 1007,
    'Owens': 1005,
    'Seneca': 1041,
}

# Rebate exclusions - ref field prefixes to ignore
REBATE_EXCLUDED_PREFIXES = ('Retro', 'Income')

# Rebate exclusions - specific ref values to ignore
REBATE_EXCLUDED_REFS = ('Owens/Retro Rebate FY 2025',)
