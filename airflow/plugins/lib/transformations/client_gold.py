"""
Gold layer transformations for client data.

Transforms bronze data into business-ready fact and dimension tables:
- fact_invoice: Invoice line items with calculated margins
- fact_invoice_detail: Invoice to customer mapping
- fact_order_item: Order details
- dim_customer: Customer dimension with normalized names
- margin_invoice: Denormalized reporting table
- fact_general_ledger: General ledger with rebate customer extraction
- margin_rebate: Rebate aggregation by customer
"""
import pandas as pd
from datetime import datetime


# Customer ID normalization - consolidate related customer IDs
CUSTOMER_ID_MAPPING = {
    1064: 1015,  # CVS/Omnicare → CVS
    1314: 1005,  # Owens variant → Owens
}

# Rebate customer name to ID mapping (for account 422200)
REBATE_CUSTOMER_ID_MAPPING = {
    'Mckesson': 1004,
    'Cardinal': 1008,
    'Schein': 1030,
    'Medline': 1007,
    'Owens': 1005,
    'Seneca': 1041,
}

# Customer name normalization - standardize customer names
CUSTOMER_NAME_MAPPING = {
    'CVS CAREMARK': 'CVS/Omnicare',
    'OMNICARE': 'CVS/Omnicare',
    'Kaleida Health': 'Olean General',
    'METHODIST HOSPITAL WESTOVER HILLS / San Antonio Supply Chain': 'Methodist Hospital Westover Hills',
    'Methodist Le Bonheur Healthcare': 'Methodist Healthcare',
    "O&M Main Street": 'Owens',
    'Owens & Minor': 'Owens',
}


def _normalize_customer_id(cust_num):
    """Normalize customer ID using mapping."""
    return CUSTOMER_ID_MAPPING.get(cust_num, cust_num)


def _normalize_customer_name(name):
    """Normalize customer name using mapping."""
    if pd.isna(name):
        return name
    return CUSTOMER_NAME_MAPPING.get(name, name)


def transform_fact_invoice(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform invoice_item → fact_invoice.

    Source: bronze/client/invoice_item (from inv_item_mst)

    Calculations:
    - ExtendedPrice = qty_invoiced * price
    - TotalCost = qty_invoiced * cost
    - Margin = qty_invoiced * (price - cost)
    """
    result = pd.DataFrame({
        'InvoiceID': df['inv_num'].astype(str).str.strip(),
        'InvoiceDate': pd.to_datetime(df['tax_date']),
        'QtyInvoiced': df['qty_invoiced'],
        'ExtendedPrice': df['qty_invoiced'] * df['price'],
        'TotalCost': df['qty_invoiced'] * df['cost'],
        'Margin': df['qty_invoiced'] * (df['price'] - df['cost']),
    })
    return result


def transform_fact_invoice_detail(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform invoice_header → fact_invoice_detail.

    Source: bronze/client/invoice_header (from inv_hdr_mst)

    Includes customer ID normalization (CVS/Omnicare → 1015, Owens → 1005).
    """
    result = pd.DataFrame({
        'InvoiceID': df['inv_num'],
        'CustomerID': df['cust_num'].apply(_normalize_customer_id),
    })
    return result


def transform_fact_order_item(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform order_item → fact_order_item.

    Source: bronze/client/order_item (from coitem_mst)
    """
    result = pd.DataFrame({
        'OrderNumber': df['co_num'],
        'OrderLine': df['co_line'],
        'ProductID': df['item'],
        'QtyOrdered': df['qty_ordered'],
        'QtyShipped': df['qty_shipped'],
        'Cost': df['cost'],
        'Price': df['price'],
        'ProductName': df['description'],
    })
    return result


def transform_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform customer_address → dim_customer.

    Source: bronze/client/customer_address (from custaddr_mst)

    Includes customer name normalization for reporting consistency.
    """
    result = pd.DataFrame({
        'CustomerID': df['cust_num'],
        'CustomerSequence': df['cust_seq'],
        'CustomerName': df['name'].apply(_normalize_customer_name),
    })
    return result


def create_margin_invoice(
    invoice_item: pd.DataFrame,
    invoice_header: pd.DataFrame,
    order_item: pd.DataFrame,
    customer_address: pd.DataFrame
) -> pd.DataFrame:
    """
    Create margin_invoice by joining all bronze tables.

    Join Logic:
    - invoice_item (base)
    - LEFT JOIN invoice_header ON inv_num
    - LEFT JOIN order_item ON co_num, co_line
    - LEFT JOIN customer_address ON cust_num WHERE cust_seq = 0

    This is the main reporting table with full margin calculations.
    """
    # Start with invoice_item as base
    result = invoice_item.copy()

    # LEFT JOIN invoice_header to get customer info
    result = result.merge(
        invoice_header[['inv_num', 'cust_num']],
        on='inv_num',
        how='left'
    )

    # LEFT JOIN order_item to get product description
    if 'co_num' in result.columns and 'co_line' in result.columns:
        result = result.merge(
            order_item[['co_num', 'co_line', 'description']],
            on=['co_num', 'co_line'],
            how='left'
        )
    else:
        result['description'] = None

    # LEFT JOIN customer_address (cust_seq = 0 for primary address only)
    primary_customers = customer_address[customer_address['cust_seq'] == 0].copy()
    result = result.merge(
        primary_customers[['cust_num', 'name']],
        on='cust_num',
        how='left'
    )

    # Build final output with transformations
    output = pd.DataFrame({
        'Invoice': result['inv_num'].astype(str).str.strip(),
        'CustomerID': result['cust_num'].apply(_normalize_customer_id),
        'CustomerName': result['name'].apply(_normalize_customer_name),
        'InvoiceDate': pd.to_datetime(result['tax_date']).dt.date,
        'ProductID': result.get('item'),
        'ProductName': result.get('description'),
        'QtyInvoiced': result['qty_invoiced'],
        'ExtendedPrice': result['qty_invoiced'] * result['price'],
        'TotalCost': result['qty_invoiced'] * result['cost'],
        'Margin': result['qty_invoiced'] * (result['price'] - result['cost']),
    })

    return output


def _extract_rebate_customer_name(ref, acct, trans_date):
    """
    Extract rebate customer name from ref field.

    Only applies to:
    - Account 422200 (sales rebate)
    - Transactions from last year onwards
    - ref field contains a space (has customer name prefix)
    - Not 'Retro' or 'Income' prefixes
    - Not 'Owens/Retro Rebate FY 2025' specifically
    """
    if pd.isna(ref) or pd.isna(acct) or pd.isna(trans_date):
        return ''

    # Only process account 422200
    if str(acct) != '422200':
        return ''

    # Check date is from last year onwards
    try:
        if isinstance(trans_date, str):
            trans_dt = pd.to_datetime(trans_date)
        else:
            trans_dt = trans_date
        cutoff = datetime(datetime.now().year - 1, 1, 1)
        if trans_dt < cutoff:
            return ''
    except:
        return ''

    # Skip specific exclusions
    if ref == 'Owens/Retro Rebate FY 2025':
        return ''

    # Extract first word (before space)
    if ' ' in ref:
        first_word = ref.split(' ')[0]
        # Skip 'Retro' and 'Income' prefixes
        if first_word in ('Retro', 'Income'):
            return ''
        return first_word

    return ''


def _get_rebate_customer_id(customer_name, acct, trans_date):
    """Map rebate customer name to customer ID."""
    if not customer_name or pd.isna(customer_name):
        return ''

    # Only for account 422200 and recent transactions
    if str(acct) != '422200':
        return ''

    try:
        if isinstance(trans_date, str):
            trans_dt = pd.to_datetime(trans_date)
        else:
            trans_dt = trans_date
        cutoff = datetime(datetime.now().year - 1, 1, 1)
        if trans_dt < cutoff:
            return ''
    except:
        return ''

    return REBATE_CUSTOMER_ID_MAPPING.get(customer_name, '')


def transform_fact_general_ledger(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform general_ledger → fact_general_ledger.

    Source: bronze/client/general_ledger (from ledger_mst)

    Extracts rebate customer information from ref field for account 422200.
    """
    # Extract rebate customer name and ID
    rebate_names = df.apply(
        lambda row: _extract_rebate_customer_name(
            row.get('ref'), row.get('acct'), row.get('trans_date')
        ), axis=1
    )

    rebate_ids = df.apply(
        lambda row: _get_rebate_customer_id(
            _extract_rebate_customer_name(
                row.get('ref'), row.get('acct'), row.get('trans_date')
            ),
            row.get('acct'),
            row.get('trans_date')
        ), axis=1
    )

    result = pd.DataFrame({
        'Account': df['acct'],
        'Transaction_Date': pd.to_datetime(df['trans_date']),
        'Amount': df['dom_amount'],
        'Ref': df['ref'],
        'FromID': df['from_id'],
        'RebateCustomerName': rebate_names,
        'RebateCustomerID': rebate_ids,
    })

    return result


def create_margin_rebate(fact_general_ledger: pd.DataFrame) -> pd.DataFrame:
    """
    Create margin_rebate by aggregating rebates from fact_general_ledger.

    Filters to account 422200 (sales rebate) with valid rebate customer names,
    then groups by customer to sum amounts.
    """
    # Filter to rebate account with valid customer names
    filtered = fact_general_ledger[
        (fact_general_ledger['Account'].astype(str) == '422200') &
        (fact_general_ledger['RebateCustomerName'] != '') &
        (fact_general_ledger['RebateCustomerName'].notna())
    ].copy()

    if len(filtered) == 0:
        return pd.DataFrame(columns=['RebateCustomerName', 'RebateCustomerID', 'Amount'])

    # Aggregate by customer
    result = filtered.groupby(
        ['RebateCustomerName', 'RebateCustomerID'],
        as_index=False
    ).agg({'Amount': 'sum'})

    return result
