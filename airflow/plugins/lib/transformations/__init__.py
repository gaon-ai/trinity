"""
Transformation functions for gold layer tables.
"""
from lib.transformations.client_gold import (
    transform_fact_invoice,
    transform_fact_invoice_detail,
    transform_fact_order_item,
    transform_dim_customer,
    create_margin_invoice,
    transform_fact_general_ledger,
    create_margin_rebate,
    CUSTOMER_ID_MAPPING,
    CUSTOMER_NAME_MAPPING,
    REBATE_CUSTOMER_ID_MAPPING,
)

__all__ = [
    'transform_fact_invoice',
    'transform_fact_invoice_detail',
    'transform_fact_order_item',
    'transform_dim_customer',
    'create_margin_invoice',
    'transform_fact_general_ledger',
    'create_margin_rebate',
    'CUSTOMER_ID_MAPPING',
    'CUSTOMER_NAME_MAPPING',
    'REBATE_CUSTOMER_ID_MAPPING',
]
