"""
Margin invoice reporting table transformation.

Creates a denormalized table by joining all bronze client tables.
This is the primary reporting table for margin analysis.
"""
import pandas as pd

from lib.transformations._helpers import normalize_customer_id, normalize_customer_name

__all__ = ['create_margin_invoice']


def create_margin_invoice(
    invoice_item: pd.DataFrame,
    invoice_header: pd.DataFrame,
    order_item: pd.DataFrame,
    customer_address: pd.DataFrame
) -> pd.DataFrame:
    """
    Create margin_invoice by joining all bronze tables.

    This denormalized table supports margin reporting by combining:
    - Invoice line items (quantities, prices, costs)
    - Invoice header (customer linkage)
    - Order items (product descriptions)
    - Customer address (customer names)

    Join Logic:
        invoice_item (base)
        LEFT JOIN invoice_header ON inv_num
        LEFT JOIN order_item ON co_num, co_line
        LEFT JOIN customer_address ON cust_num WHERE cust_seq = 0

    Args:
        invoice_item: Bronze invoice_item DataFrame
        invoice_header: Bronze invoice_header DataFrame
        order_item: Bronze order_item DataFrame
        customer_address: Bronze customer_address DataFrame

    Returns:
        Denormalized DataFrame with margin calculations
    """
    result = invoice_item.copy()

    # LEFT JOIN invoice_header
    result = result.merge(
        invoice_header[['inv_num', 'cust_num']],
        on='inv_num',
        how='left'
    )

    # LEFT JOIN order_item (for product description)
    if 'co_num' in result.columns and 'co_line' in result.columns:
        result = result.merge(
            order_item[['co_num', 'co_line', 'description']],
            on=['co_num', 'co_line'],
            how='left'
        )
    else:
        result['description'] = None

    # LEFT JOIN customer_address (primary address only: cust_seq = 0)
    primary_customers = customer_address[customer_address['cust_seq'] == 0]
    result = result.merge(
        primary_customers[['cust_num', 'name']],
        on='cust_num',
        how='left'
    )

    return pd.DataFrame({
        'Invoice': result['inv_num'].astype(str).str.strip(),
        'CustomerID': result['cust_num'].apply(normalize_customer_id),
        'CustomerName': result['name'].apply(normalize_customer_name),
        'InvoiceDate': pd.to_datetime(result['tax_date']).dt.date,
        'ProductID': result.get('item'),
        'ProductName': result.get('description'),
        'QtyInvoiced': result['qty_invoiced'],
        'ExtendedPrice': result['qty_invoiced'] * result['price'],
        'TotalCost': result['qty_invoiced'] * result['cost'],
        'Margin': result['qty_invoiced'] * (result['price'] - result['cost']),
    })
