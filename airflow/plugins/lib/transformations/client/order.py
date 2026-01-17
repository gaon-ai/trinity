"""
Order fact table transformations.

Source tables:
- order_item (coitem_mst) -> fact_order_item
"""
import pandas as pd


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
        'OrderLine': df['co_line'],
        'ProductID': df['item'],
        'QtyOrdered': df['qty_ordered'],
        'QtyShipped': df['qty_shipped'],
        'Cost': df['cost'],
        'Price': df['price'],
        'ProductName': df['description'],
    })
