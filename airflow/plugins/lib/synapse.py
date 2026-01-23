"""
Synapse Serverless SQL operations.

This module handles creating and updating views in Synapse
that point to gold layer data in the Data Lake.
"""
import os
from typing import Dict, List, Optional

__all__ = ['create_gold_views', 'get_synapse_config']


# View definitions for client gold tables
# Format: {view_name: {bulk_path, columns, select (optional)}}
# - bulk_path: Path pattern to CSV files in Data Lake
# - columns: WITH clause column definitions for OPENROWSET
# - select: Optional custom SELECT clause (for type conversions), defaults to *
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
        # RebateCustomerID stored as float in CSV, need VARCHAR + CAST
        'columns': """
            Account VARCHAR(20),
            Transaction_Date DATE,
            Domestic_Amount DECIMAL(18,4),
            Foreign_Amount DECIMAL(18,4),
            FromID VARCHAR(50),
            Reference VARCHAR(200),
            RebateCustomerName VARCHAR(100),
            RebateCustomerID VARCHAR(20)
        """,
        'select': """
            Account,
            Transaction_Date,
            Domestic_Amount,
            Foreign_Amount,
            FromID,
            Reference,
            RebateCustomerName,
            CAST(CAST(NULLIF(RebateCustomerID, '') AS DECIMAL(10,0)) AS INT) as RebateCustomerID
        """,
    },
    'margin_rebate': {
        'bulk_path': 'gold/margin_rebate/*/data.csv',
        # RebateCustomerID stored as float in CSV, need VARCHAR + CAST
        'columns': """
            RebateCustomerName VARCHAR(100),
            RebateCustomerID VARCHAR(20),
            Domestic_Amount DECIMAL(18,4)
        """,
        'select': """
            RebateCustomerName,
            CAST(CAST(NULLIF(RebateCustomerID, '') AS DECIMAL(10,0)) AS INT) as RebateCustomerID,
            Domestic_Amount
        """,
    },
    'dim_lessee': {
        'bulk_path': 'gold/dim_lessee/*/data.csv',
        'columns': """
            lessee_number VARCHAR(50),
            customer_number VARCHAR(50),
            lessee_name VARCHAR(200),
            lessee_address_1 VARCHAR(200),
            lessee_address_2 VARCHAR(200),
            lessee_city VARCHAR(100),
            lessee_state_code VARCHAR(10),
            lessee_zip_code VARCHAR(20),
            lessee_telephone VARCHAR(30),
            lessee_cellular VARCHAR(30),
            business_type_description VARCHAR(100),
            lessee_birth_date DATE,
            lessee_language VARCHAR(20),
            email_address VARCHAR(200),
            contract_count INT,
            first_contract_date DATE,
            last_contract_date DATE,
            last_updated DATETIME2
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
                # Use custom SELECT if defined (for type conversions), else SELECT *
                select_clause = view_def.get('select', '*')
                cursor.execute(f"""
                    CREATE VIEW {view_name} AS
                    SELECT {select_clause}
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
