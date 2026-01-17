"""
Order fact table transformations.

Source tables:
- order_item (coitem_mst) -> fact_order_item
"""
import pandas as pd

__all__ = ['transform_fact_order_item']


def transform_fact_order_item(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform order_item -> fact_order_item.

    Source: bronze/client/order_item (from coitem_mst)
    Primary Key: co_num, co_line

    Columns:
        OrderNumber: Customer order number
        OrderLine: Order line number
        ProductID: Item code
        QtyOrdered: Quantity ordered
        QtyShipped: Quantity shipped
        Cost: Unit cost
        Price: Unit price
        ProductName: Item description
    """
    return pd.DataFrame({
        'OrderNumber': df['co_num'],
        'OrderLine': pd.to_numeric(df['co_line'], errors='coerce'),
        'ProductID': df['item'],
        'QtyOrdered': pd.to_numeric(df['qty_ordered'], errors='coerce').fillna(0),
        'QtyShipped': pd.to_numeric(df['qty_shipped'], errors='coerce').fillna(0),
        'Cost': pd.to_numeric(df['cost'], errors='coerce').fillna(0),
        'Price': pd.to_numeric(df['price'], errors='coerce').fillna(0),
        'ProductName': df['description'],
    })
