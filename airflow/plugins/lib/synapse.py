"""
Synapse Serverless SQL operations.

This module handles creating and updating views in Synapse
that point to gold layer data in the Data Lake.
"""
import os
from typing import Dict, List, Optional

__all__ = ['create_gold_views', 'get_synapse_config']


# View definitions for client gold tables
# Format: {view_name: {bulk_path, columns}}
CLIENT_GOLD_VIEWS = {
    'fact_invoice': {
        'bulk_path': 'gold/fact_invoice/*/data.csv',
        'columns': """
            InvoiceID VARCHAR(50),
            InvoiceDate DATE,
            QtyInvoiced DECIMAL(18,4),
            ExtendedPrice DECIMAL(18,4),
            TotalCost DECIMAL(18,4),
            Margin DECIMAL(18,4)
        """,
    },
    'fact_invoice_detail': {
        'bulk_path': 'gold/fact_invoice_detail/*/data.csv',
        'columns': """
            InvoiceID VARCHAR(50),
            CustomerID INT
        """,
    },
    'fact_order_item': {
        'bulk_path': 'gold/fact_order_item/*/data.csv',
        'columns': """
            OrderNumber VARCHAR(50),
            OrderLine INT,
            ProductID VARCHAR(50),
            QtyOrdered DECIMAL(18,4),
            QtyShipped DECIMAL(18,4),
            Cost DECIMAL(18,4),
            Price DECIMAL(18,4),
            ProductName VARCHAR(200)
        """,
    },
    'dim_customer': {
        'bulk_path': 'gold/dim_customer/*/data.csv',
        'columns': """
            CustomerID INT,
            CustomerSequence INT,
            CustomerName VARCHAR(200)
        """,
    },
    'margin_invoice': {
        'bulk_path': 'gold/margin_invoice/*/data.csv',
        'columns': """
            Invoice VARCHAR(50),
            CustomerID INT,
            CustomerName VARCHAR(200),
            InvoiceDate DATE,
            ProductID VARCHAR(50),
            ProductName VARCHAR(200),
            QtyInvoiced DECIMAL(18,4),
            ExtendedPrice DECIMAL(18,4),
            TotalCost DECIMAL(18,4),
            Margin DECIMAL(18,4)
        """,
    },
    'fact_general_ledger': {
        'bulk_path': 'gold/fact_general_ledger/*/data.csv',
        'columns': """
            Account VARCHAR(20),
            Transaction_Date DATE,
            Domestic_Amount DECIMAL(18,4),
            Foreign_Amount DECIMAL(18,4),
            FromID VARCHAR(50),
            Reference VARCHAR(200),
            RebateCustomerName VARCHAR(100),
            RebateCustomerID INT
        """,
    },
    'margin_rebate': {
        'bulk_path': 'gold/margin_rebate/*/data.csv',
        'columns': """
            RebateCustomerName VARCHAR(100),
            RebateCustomerID INT,
            Domestic_Amount DECIMAL(18,4)
        """,
    },
}


def get_synapse_config() -> Dict[str, str]:
    """Get Synapse connection configuration from environment."""
    return {
        'server': os.environ.get('SYNAPSE_SERVER', ''),
        'user': os.environ.get('SYNAPSE_USER', 'sqladmin'),
        'password': os.environ.get('SYNAPSE_PASSWORD', ''),
        'database': os.environ.get('SYNAPSE_DATABASE', 'trinity'),
    }


def _is_synapse_configured() -> bool:
    """Check if Synapse credentials are configured."""
    config = get_synapse_config()
    return bool(config['server'] and config['password'])


def create_gold_views(views: Optional[List[str]] = None) -> Dict[str, str]:
    """
    Create or update Synapse views for gold tables.

    Args:
        views: List of view names to create. If None, creates all views.

    Returns:
        Dict with view names and their status ('created', 'updated', 'skipped', 'error')
    """
    if not _is_synapse_configured():
        print("Synapse not configured (SYNAPSE_SERVER/SYNAPSE_PASSWORD not set)")
        print("Skipping view creation. Views can be created manually with scripts/synapse_setup.py")
        return {name: 'skipped' for name in (views or CLIENT_GOLD_VIEWS.keys())}

    try:
        import pymssql
    except ImportError:
        print("pymssql not installed. Skipping Synapse view creation.")
        return {name: 'skipped' for name in (views or CLIENT_GOLD_VIEWS.keys())}

    config = get_synapse_config()
    results = {}

    views_to_create = views or list(CLIENT_GOLD_VIEWS.keys())

    print(f"Connecting to Synapse: {config['server']}")

    try:
        conn = pymssql.connect(
            server=config['server'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            autocommit=True
        )
        cursor = conn.cursor()

        for view_name in views_to_create:
            if view_name not in CLIENT_GOLD_VIEWS:
                print(f"  Unknown view: {view_name}")
                results[view_name] = 'error'
                continue

            view_def = CLIENT_GOLD_VIEWS[view_name]
            print(f"  Creating view: {view_name}...")

            try:
                cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
                cursor.execute(f"""
                    CREATE VIEW {view_name} AS
                    SELECT *
                    FROM OPENROWSET(
                        BULK '{view_def['bulk_path']}',
                        DATA_SOURCE = 'TrinityLake',
                        FORMAT = 'CSV',
                        PARSER_VERSION = '2.0',
                        FIRSTROW = 2
                    ) WITH ({view_def['columns']}) AS data
                """)
                print(f"  ✓ {view_name} created")
                results[view_name] = 'created'
            except pymssql.Error as e:
                print(f"  ✗ {view_name} error: {e}")
                results[view_name] = 'error'

        conn.close()

    except pymssql.Error as e:
        print(f"Synapse connection error: {e}")
        return {name: 'error' for name in views_to_create}

    return results
