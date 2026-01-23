"""
Gold layer transformation functions.

This package provides transformation functions for converting bronze layer
data into gold layer fact and dimension tables.

Structure:
    _constants.py   - Shared business constants (mappings, account codes)
    _helpers.py     - Shared helper functions (normalization, validation)
    client/         - Client SQL Server transformations

Usage:
    # Import specific transformations
    from lib.transformations.client import (
        transform_fact_invoice,
        transform_dim_customer,
        create_margin_invoice,
    )

    # Or import constants for reference/testing
    from lib.transformations import (
        CUSTOMER_ID_MAPPING,
        CUSTOMER_NAME_MAPPING,
    )

Extending:
    1. New client table: Add module to client/ directory
    2. New data source: Create new subdirectory (e.g., erp/, crm/)
    3. New mapping: Add to _constants.py
    4. New helper: Add to _helpers.py
"""

# Re-export constants for backward compatibility and convenience
from lib.transformations._constants import (
    SALES_REBATE_ACCOUNT,
    CUSTOMER_ID_MAPPING,
    CUSTOMER_NAME_MAPPING,
    REBATE_CUSTOMER_ID_MAPPING,
    REBATE_EXCLUDED_PREFIXES,
    REBATE_EXCLUDED_REFS,
)

# Re-export client transformations for convenience
# (can also import directly from lib.transformations.client)
from lib.transformations.client import (
    transform_fact_invoice,
    transform_fact_invoice_detail,
    transform_fact_order_item,
    transform_dim_customer,
    transform_fact_general_ledger,
    create_margin_invoice,
    create_margin_rebate,
)

__all__ = [
    # Constants
    'SALES_REBATE_ACCOUNT',
    'CUSTOMER_ID_MAPPING',
    'CUSTOMER_NAME_MAPPING',
    'REBATE_CUSTOMER_ID_MAPPING',
    'REBATE_EXCLUDED_PREFIXES',
    'REBATE_EXCLUDED_REFS',
    # Client transformations
    'transform_fact_invoice',
    'transform_fact_invoice_detail',
    'transform_fact_order_item',
    'transform_dim_customer',
    'transform_fact_general_ledger',
    'create_margin_invoice',
    'create_margin_rebate',
]
