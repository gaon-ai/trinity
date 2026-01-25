"""
Client SQL Server table configurations.

Tables from the client's on-prem SQL Server database (Prod_Trinity_App),
accessible via site-to-site VPN.

Tables are organized by domain:
- inventory.py: Invoice and inventory data
- orders.py: Customer order data
- customers.py: Customer master data
- finance.py: General ledger and financial data
"""
from lib.tables.client.inventory import INVENTORY_TABLES
from lib.tables.client.orders import ORDER_TABLES
from lib.tables.client.customers import CUSTOMER_TABLES
from lib.tables.client.finance import FINANCE_TABLES

# Combined list of all client tables
ALL_TABLES = INVENTORY_TABLES + ORDER_TABLES + CUSTOMER_TABLES + FINANCE_TABLES

# Backward compatibility exports
INCREMENTAL_TABLES = [t for t in ALL_TABLES if t.is_incremental]
FULL_REFRESH_TABLES = [t for t in ALL_TABLES if not t.is_incremental]

__all__ = [
    # Domain-specific lists
    "INVENTORY_TABLES",
    "ORDER_TABLES",
    "CUSTOMER_TABLES",
    "FINANCE_TABLES",
    # Combined list
    "ALL_TABLES",
    # Backward compatibility
    "INCREMENTAL_TABLES",
    "FULL_REFRESH_TABLES",
]
