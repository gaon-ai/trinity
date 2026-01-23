"""
Client data transformations (Bronze -> Gold).

Source: Client SQL Server (Prod_Trinity_App)
Target: Gold layer fact and dimension tables

Modules:
- invoice: Invoice fact tables (fact_invoice, fact_invoice_detail)
- order: Order fact tables (fact_order_item)
- customer: Customer dimension (dim_customer)
- lessee: Lessee dimension (dim_lessee)
- ledger: General ledger facts and rebates (fact_general_ledger, margin_rebate)
- margin_invoice: Denormalized reporting table joining all sources
"""
from lib.transformations.client.invoice import transform_fact_invoice, transform_fact_invoice_detail
from lib.transformations.client.order import transform_fact_order_item
from lib.transformations.client.customer import transform_dim_customer
from lib.transformations.client.lessee import create_dim_lessee
from lib.transformations.client.ledger import transform_fact_general_ledger, create_margin_rebate
from lib.transformations.client.margin_invoice import create_margin_invoice

__all__ = [
    # Fact tables
    'transform_fact_invoice',
    'transform_fact_invoice_detail',
    'transform_fact_order_item',
    'transform_fact_general_ledger',
    # Dimension tables
    'transform_dim_customer',
    'create_dim_lessee',
    # Reporting tables
    'create_margin_invoice',
    'create_margin_rebate',
]
