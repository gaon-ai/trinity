"""
Customer dimension table transformations.

Source tables:
- customer_address (custaddr_mst) -> dim_customer
"""
import pandas as pd

from .._helpers import normalize_customer_name

__all__ = ['transform_dim_customer']


def transform_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform customer_address -> dim_customer.

    Source: bronze/client/customer_address (from custaddr_mst)
    Primary Key: cust_num, cust_seq

    Columns:
        CustomerID: Customer number
        CustomerSequence: Address sequence (0 = primary)
        CustomerName: Customer name (normalized)
    """
    return pd.DataFrame({
        'CustomerID': df['cust_num'],
        'CustomerSequence': df['cust_seq'],
        'CustomerName': df['name'].apply(normalize_customer_name),
    })
