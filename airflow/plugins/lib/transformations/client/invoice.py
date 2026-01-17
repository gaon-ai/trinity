"""
Invoice fact table transformations.

Source tables:
- invoice_item (inv_item_mst) -> fact_invoice
- invoice_header (inv_hdr_mst) -> fact_invoice_detail
"""
import pandas as pd

from .._helpers import normalize_customer_id


def transform_fact_invoice(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform invoice_item -> fact_invoice.

    Source: bronze/client/invoice_item (from inv_item_mst)
    Primary Key: inv_num, inv_line, co_line

    Columns:
        InvoiceID: Invoice number (trimmed)
        InvoiceDate: Tax date
        QtyInvoiced: Quantity invoiced
        ExtendedPrice: qty_invoiced * price
        TotalCost: qty_invoiced * cost
        Margin: qty_invoiced * (price - cost)
    """
    return pd.DataFrame({
        'InvoiceID': df['inv_num'].astype(str).str.strip(),
        'InvoiceDate': pd.to_datetime(df['tax_date']),
        'QtyInvoiced': df['qty_invoiced'],
        'ExtendedPrice': df['qty_invoiced'] * df['price'],
        'TotalCost': df['qty_invoiced'] * df['cost'],
        'Margin': df['qty_invoiced'] * (df['price'] - df['cost']),
    })


def transform_fact_invoice_detail(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform invoice_header -> fact_invoice_detail.

    Source: bronze/client/invoice_header (from inv_hdr_mst)
    Primary Key: inv_num

    Columns:
        InvoiceID: Invoice number
        CustomerID: Customer number (normalized)
    """
    return pd.DataFrame({
        'InvoiceID': df['inv_num'],
        'CustomerID': df['cust_num'].apply(normalize_customer_id),
    })
