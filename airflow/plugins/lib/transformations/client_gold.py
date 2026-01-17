"""
Gold layer transformations for client data.

Transforms bronze data into business-ready fact and dimension tables.
Business logic (mappings, calculations) is separated from transformation functions.
"""
import pandas as pd
from datetime import datetime
from typing import Optional


# =============================================================================
# BUSINESS CONSTANTS
# =============================================================================

# Account codes
SALES_REBATE_ACCOUNT = '422200'

# Customer ID normalization - consolidate related customer IDs
CUSTOMER_ID_MAPPING = {
    1064: 1015,  # CVS/Omnicare → CVS
    1314: 1005,  # Owens variant → Owens
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

# Rebate customer name to ID mapping (for sales rebate account)
REBATE_CUSTOMER_ID_MAPPING = {
    'Mckesson': 1004,
    'Cardinal': 1008,
    'Schein': 1030,
    'Medline': 1007,
    'Owens': 1005,
    'Seneca': 1041,
}

# Rebate exclusions
REBATE_EXCLUDED_PREFIXES = ('Retro', 'Income')
REBATE_EXCLUDED_REFS = ('Owens/Retro Rebate FY 2025',)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def normalize_customer_id(cust_num):
    """Normalize customer ID using mapping."""
    return CUSTOMER_ID_MAPPING.get(cust_num, cust_num)


def normalize_customer_name(name):
    """Normalize customer name using mapping."""
    if pd.isna(name):
        return name
    return CUSTOMER_NAME_MAPPING.get(name, name)


def get_rebate_cutoff_date() -> datetime:
    """Get cutoff date for rebate processing (Jan 1 of last year)."""
    return datetime(datetime.now().year - 1, 1, 1)


def is_rebate_eligible(acct, trans_date) -> bool:
    """Check if a transaction is eligible for rebate extraction."""
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
    Extract customer name from ref field (first word before space).

    Returns empty string if:
    - ref is empty/null
    - ref doesn't contain a space
    - First word is an excluded prefix (Retro, Income)
    - ref is in exclusion list
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
    """Map rebate customer name to customer ID."""
    if not customer_name:
        return None
    return REBATE_CUSTOMER_ID_MAPPING.get(customer_name)


# =============================================================================
# FACT TABLE TRANSFORMATIONS
# =============================================================================

def transform_fact_invoice(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform invoice_item → fact_invoice.

    Source: bronze/client/invoice_item (from inv_item_mst)
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
    Transform invoice_header → fact_invoice_detail.

    Source: bronze/client/invoice_header (from inv_hdr_mst)
    """
    return pd.DataFrame({
        'InvoiceID': df['inv_num'],
        'CustomerID': df['cust_num'].apply(normalize_customer_id),
    })


def transform_fact_order_item(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform order_item → fact_order_item.

    Source: bronze/client/order_item (from coitem_mst)
    """
    return pd.DataFrame({
        'OrderNumber': df['co_num'],
        'OrderLine': df['co_line'],
        'ProductID': df['item'],
        'QtyOrdered': df['qty_ordered'],
        'QtyShipped': df['qty_shipped'],
        'Cost': df['cost'],
        'Price': df['price'],
        'ProductName': df['description'],
    })


def transform_fact_general_ledger(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform general_ledger → fact_general_ledger.

    Source: bronze/client/general_ledger (from ledger_mst)

    Extracts rebate customer information from ref field for sales rebate account.
    """
    # Vectorized rebate extraction
    def extract_rebate_info(row):
        acct = row.get('acct')
        trans_date = row.get('trans_date')
        ref = row.get('ref')

        if not is_rebate_eligible(acct, trans_date):
            return '', None

        customer_name = extract_rebate_customer_name(ref)
        customer_id = get_rebate_customer_id(customer_name) if customer_name else None

        return customer_name, customer_id

    # Apply extraction
    rebate_info = df.apply(extract_rebate_info, axis=1, result_type='expand')
    rebate_info.columns = ['RebateCustomerName', 'RebateCustomerID']

    return pd.DataFrame({
        'Account': df['acct'],
        'Transaction_Date': pd.to_datetime(df['trans_date']),
        'Domestic_Amount': df['dom_amount'],
        'Foreign_Amount': df['for_amount'],
        'FromID': df['from_id'],
        'Reference': df['ref'],
        'RebateCustomerName': rebate_info['RebateCustomerName'],
        'RebateCustomerID': rebate_info['RebateCustomerID'],
    })


# =============================================================================
# DIMENSION TABLE TRANSFORMATIONS
# =============================================================================

def transform_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform customer_address → dim_customer.

    Source: bronze/client/customer_address (from custaddr_mst)
    """
    return pd.DataFrame({
        'CustomerID': df['cust_num'],
        'CustomerSequence': df['cust_seq'],
        'CustomerName': df['name'].apply(normalize_customer_name),
    })


# =============================================================================
# REPORTING TABLE TRANSFORMATIONS
# =============================================================================

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
    """
    result = invoice_item.copy()

    # LEFT JOIN invoice_header
    result = result.merge(
        invoice_header[['inv_num', 'cust_num']],
        on='inv_num',
        how='left'
    )

    # LEFT JOIN order_item
    if 'co_num' in result.columns and 'co_line' in result.columns:
        result = result.merge(
            order_item[['co_num', 'co_line', 'description']],
            on=['co_num', 'co_line'],
            how='left'
        )
    else:
        result['description'] = None

    # LEFT JOIN customer_address (primary address only)
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


def create_margin_rebate(fact_general_ledger: pd.DataFrame) -> pd.DataFrame:
    """
    Create margin_rebate by aggregating rebates from fact_general_ledger.

    Filters to sales rebate account with valid customer names,
    then groups by customer to sum amounts.
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
