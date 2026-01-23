#!/usr/bin/env python3
"""
Synapse Serverless SQL Setup Script

This script configures Synapse to query Data Lake files.
Run this after creating the Synapse workspace with 04-create-synapse.sh

Usage:
    export SYNAPSE_PASSWORD="your-password"
    export STORAGE_ACCOUNT_KEY="your-storage-key"
    python3 scripts/synapse_setup.py
"""

import os
import sys
import subprocess

# Configuration - set via environment variables
CONFIG = {
    "server": os.environ.get("SYNAPSE_SERVER", ""),
    "user": os.environ.get("SYNAPSE_USER", "sqladmin"),
    "password": os.environ.get("SYNAPSE_PASSWORD", ""),
    "database": os.environ.get("SYNAPSE_DATABASE", "trinity"),
    "storage_account": os.environ.get("STORAGE_ACCOUNT_NAME", ""),
    "storage_key": os.environ.get("STORAGE_ACCOUNT_KEY", ""),
    "master_key_password": os.environ.get("MASTER_KEY_PASSWORD", "Trinity2025Secure!"),
}


def get_sas_token(storage_account: str, storage_key: str) -> str:
    """Generate a SAS token using Azure CLI."""
    result = subprocess.run(
        [
            "az", "storage", "account", "generate-sas",
            "--account-name", storage_account,
            "--account-key", storage_key,
            "--services", "bf",
            "--resource-types", "sco",
            "--permissions", "rl",
            "--expiry", "2026-12-31T00:00:00Z",
            "--https-only",
            "-o", "tsv"
        ],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to generate SAS token: {result.stderr}")
    return result.stdout.strip()


def setup_synapse():
    """Set up Synapse credentials and data source."""
    try:
        import pymssql
    except ImportError:
        print("Installing pymssql...")
        subprocess.run([sys.executable, "-m", "pip", "install", "pymssql"], check=True)
        import pymssql

    if not CONFIG["password"]:
        print("Error: SYNAPSE_PASSWORD environment variable not set")
        sys.exit(1)

    if not CONFIG["storage_key"]:
        print("Error: STORAGE_ACCOUNT_KEY environment variable not set")
        sys.exit(1)

    print(f"Synapse Server: {CONFIG['server']}")
    print(f"Storage Account: {CONFIG['storage_account']}")
    print()

    # Generate SAS token
    print("Generating SAS token...")
    sas_token = get_sas_token(CONFIG["storage_account"], CONFIG["storage_key"])
    print("SAS token generated!")

    # Connect to master to create database (autocommit required for CREATE DATABASE)
    print("\nConnecting to Synapse...")
    conn = pymssql.connect(
        server=CONFIG["server"],
        user=CONFIG["user"],
        password=CONFIG["password"],
        database="master",
        autocommit=True
    )
    cursor = conn.cursor()

    # Create database
    print(f"Creating database '{CONFIG['database']}'...")
    try:
        cursor.execute(f"CREATE DATABASE {CONFIG['database']}")
        print("Database created!")
    except pymssql.Error as e:
        if "already exists" in str(e).lower():
            print("Database already exists, continuing...")
        else:
            raise
    conn.close()

    # Connect to user database (autocommit required for DDL statements)
    conn = pymssql.connect(
        server=CONFIG["server"],
        user=CONFIG["user"],
        password=CONFIG["password"],
        database=CONFIG["database"],
        autocommit=True
    )
    cursor = conn.cursor()

    # Create master key
    print("\nCreating master key...")
    try:
        cursor.execute(f"CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{CONFIG['master_key_password']}'")
        print("Master key created!")
    except pymssql.Error as e:
        if "already" in str(e).lower():
            print("Master key already exists, continuing...")
        else:
            raise

    # Create credential
    print("\nCreating credential 'TrinityStorageKey'...")
    try:
        cursor.execute(f"""
            CREATE DATABASE SCOPED CREDENTIAL TrinityStorageKey
            WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
            SECRET = '{sas_token}'
        """)
        print("Credential created!")
    except pymssql.Error as e:
        if "already exists" in str(e).lower():
            print("Credential already exists. Dropping and recreating...")
            cursor.execute("DROP DATABASE SCOPED CREDENTIAL TrinityStorageKey")
            cursor.execute(f"""
                CREATE DATABASE SCOPED CREDENTIAL TrinityStorageKey
                WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
                SECRET = '{sas_token}'
            """)
            print("Credential recreated!")
        else:
            raise

    # Create external data source
    print("\nCreating external data source 'TrinityLake'...")
    try:
        cursor.execute(f"""
            CREATE EXTERNAL DATA SOURCE TrinityLake
            WITH (
                LOCATION = 'https://{CONFIG['storage_account']}.dfs.core.windows.net',
                CREDENTIAL = TrinityStorageKey
            )
        """)
        print("External data source created!")
    except pymssql.Error as e:
        if "already exists" in str(e).lower():
            print("Data source already exists, continuing...")
        else:
            raise

    # Create views for Gold layer datasets
    print("\n" + "=" * 60)
    print("Creating views for Gold layer...")
    print("=" * 60 + "\n")

    # View: sales_by_region (queries all partitions)
    print("Creating view: sales_by_region...")
    try:
        cursor.execute("DROP VIEW IF EXISTS sales_by_region")
        cursor.execute("""
            CREATE VIEW sales_by_region AS
            SELECT *
            FROM OPENROWSET(
                BULK 'gold/sales_by_region/*/data.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
                region VARCHAR(50),
                order_count INT,
                total_quantity INT,
                total_revenue DECIMAL(18,2),
                report_date VARCHAR(50)
            ) AS data
        """)
        print("✅ View sales_by_region created!")
    except pymssql.Error as e:
        print(f"Error creating sales_by_region: {e}")

    # View: sales_by_product (queries all partitions)
    print("Creating view: sales_by_product...")
    try:
        cursor.execute("DROP VIEW IF EXISTS sales_by_product")
        cursor.execute("""
            CREATE VIEW sales_by_product AS
            SELECT *
            FROM OPENROWSET(
                BULK 'gold/sales_by_product/*/data.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
                product VARCHAR(100),
                order_count INT,
                total_quantity INT,
                total_revenue DECIMAL(18,2),
                report_date VARCHAR(50)
            ) AS data
        """)
        print("✅ View sales_by_product created!")
    except pymssql.Error as e:
        print(f"Error creating sales_by_product: {e}")

    # View: silver_sales (cleaned sales data)
    print("Creating view: silver_sales...")
    try:
        cursor.execute("DROP VIEW IF EXISTS silver_sales")
        cursor.execute("""
            CREATE VIEW silver_sales AS
            SELECT *
            FROM OPENROWSET(
                BULK 'silver/sales/*/cleaned.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
                order_id VARCHAR(50),
                customer_id VARCHAR(50),
                product VARCHAR(100),
                quantity INT,
                unit_price DECIMAL(18,2),
                order_date VARCHAR(50),
                region VARCHAR(50),
                total_amount DECIMAL(18,2),
                processed_at VARCHAR(100)
            ) AS data
        """)
        print("✅ View silver_sales created!")
    except pymssql.Error as e:
        print(f"Error creating silver_sales: {e}")

    # =========================================================================
    # CLIENT GOLD LAYER VIEWS
    # =========================================================================
    print("\n" + "=" * 60)
    print("Creating views for Client Gold layer...")
    print("=" * 60 + "\n")

    # View: fact_invoice (invoice line items with margins)
    print("Creating view: fact_invoice...")
    try:
        cursor.execute("DROP VIEW IF EXISTS fact_invoice")
        cursor.execute("""
            CREATE VIEW fact_invoice AS
            SELECT *
            FROM OPENROWSET(
                BULK 'gold/fact_invoice/*/data.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
                InvoiceID VARCHAR(50),
                InvoiceDate DATE,
                QtyInvoiced DECIMAL(18,4),
                ExtendedPrice DECIMAL(18,4),
                TotalCost DECIMAL(18,4),
                Margin DECIMAL(18,4)
            ) AS data
        """)
        print("✅ View fact_invoice created!")
    except pymssql.Error as e:
        print(f"Error creating fact_invoice: {e}")

    # View: fact_invoice_detail (invoice to customer mapping)
    print("Creating view: fact_invoice_detail...")
    try:
        cursor.execute("DROP VIEW IF EXISTS fact_invoice_detail")
        cursor.execute("""
            CREATE VIEW fact_invoice_detail AS
            SELECT *
            FROM OPENROWSET(
                BULK 'gold/fact_invoice_detail/*/data.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
                InvoiceID VARCHAR(50),
                CustomerID INT
            ) AS data
        """)
        print("✅ View fact_invoice_detail created!")
    except pymssql.Error as e:
        print(f"Error creating fact_invoice_detail: {e}")

    # View: fact_order_item (order details)
    print("Creating view: fact_order_item...")
    try:
        cursor.execute("DROP VIEW IF EXISTS fact_order_item")
        cursor.execute("""
            CREATE VIEW fact_order_item AS
            SELECT *
            FROM OPENROWSET(
                BULK 'gold/fact_order_item/*/data.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
                OrderNumber VARCHAR(50),
                OrderLine INT,
                ProductID VARCHAR(50),
                QtyOrdered DECIMAL(18,4),
                QtyShipped DECIMAL(18,4),
                Cost DECIMAL(18,4),
                Price DECIMAL(18,4),
                ProductName VARCHAR(200)
            ) AS data
        """)
        print("✅ View fact_order_item created!")
    except pymssql.Error as e:
        print(f"Error creating fact_order_item: {e}")

    # View: dim_customer (customer dimension)
    print("Creating view: dim_customer...")
    try:
        cursor.execute("DROP VIEW IF EXISTS dim_customer")
        cursor.execute("""
            CREATE VIEW dim_customer AS
            SELECT *
            FROM OPENROWSET(
                BULK 'gold/dim_customer/*/data.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
                CustomerID INT,
                CustomerSequence INT,
                CustomerName VARCHAR(200)
            ) AS data
        """)
        print("✅ View dim_customer created!")
    except pymssql.Error as e:
        print(f"Error creating dim_customer: {e}")

    # View: margin_invoice (denormalized reporting table)
    print("Creating view: margin_invoice...")
    try:
        cursor.execute("DROP VIEW IF EXISTS margin_invoice")
        cursor.execute("""
            CREATE VIEW margin_invoice AS
            SELECT *
            FROM OPENROWSET(
                BULK 'gold/margin_invoice/*/data.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
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
            ) AS data
        """)
        print("✅ View margin_invoice created!")
    except pymssql.Error as e:
        print(f"Error creating margin_invoice: {e}")

    # View: fact_general_ledger (general ledger with rebate info)
    # Note: RebateCustomerID is stored as float in CSV (e.g., "1008.0"), need type conversion
    print("Creating view: fact_general_ledger...")
    try:
        cursor.execute("DROP VIEW IF EXISTS fact_general_ledger")
        cursor.execute("""
            CREATE VIEW fact_general_ledger AS
            SELECT
                Account,
                Transaction_Date,
                Domestic_Amount,
                Foreign_Amount,
                FromID,
                Reference,
                RebateCustomerName,
                CAST(CAST(NULLIF(RebateCustomerID, '') AS DECIMAL(10,0)) AS INT) as RebateCustomerID
            FROM OPENROWSET(
                BULK 'gold/fact_general_ledger/*/data.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
                Account VARCHAR(20),
                Transaction_Date DATE,
                Domestic_Amount DECIMAL(18,4),
                Foreign_Amount DECIMAL(18,4),
                FromID VARCHAR(50),
                Reference VARCHAR(200),
                RebateCustomerName VARCHAR(100),
                RebateCustomerID VARCHAR(20)
            ) AS data
        """)
        print("✅ View fact_general_ledger created!")
    except pymssql.Error as e:
        print(f"Error creating fact_general_ledger: {e}")

    # View: margin_rebate (rebates aggregated by customer)
    # Note: RebateCustomerID is stored as float in CSV (e.g., "1008.0"), need type conversion
    print("Creating view: margin_rebate...")
    try:
        cursor.execute("DROP VIEW IF EXISTS margin_rebate")
        cursor.execute("""
            CREATE VIEW margin_rebate AS
            SELECT
                RebateCustomerName,
                CAST(CAST(NULLIF(RebateCustomerID, '') AS DECIMAL(10,0)) AS INT) as RebateCustomerID,
                Domestic_Amount
            FROM OPENROWSET(
                BULK 'gold/margin_rebate/*/data.csv',
                DATA_SOURCE = 'TrinityLake',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                FIRSTROW = 2
            ) WITH (
                RebateCustomerName VARCHAR(100),
                RebateCustomerID VARCHAR(20),
                Domestic_Amount DECIMAL(18,4)
            ) AS data
        """)
        print("✅ View margin_rebate created!")
    except pymssql.Error as e:
        print(f"Error creating margin_rebate: {e}")

    # Test queries
    print("\n" + "=" * 60)
    print("Testing views...")
    print("=" * 60 + "\n")

    # Test ERP views
    try:
        cursor.execute("SELECT TOP 5 * FROM sales_by_region")
        rows = cursor.fetchall()

        if rows:
            print("sales_by_region sample:")
            print(f"{'region':<10} {'orders':<8} {'qty':<8} {'revenue':<12} {'date':<12}")
            print("-" * 50)
            for row in rows:
                print(f"{row[0]:<10} {row[1]:<8} {row[2]:<8} ${row[3]:<11.2f} {row[4]:<12}")
            print(f"\n✅ ERP views working!")
        else:
            print("ERP views created but no data yet.")

    except pymssql.Error as e:
        print(f"ERP views not populated yet: {e}")

    # Test Client Gold views
    print()
    try:
        cursor.execute("SELECT TOP 5 * FROM margin_rebate")
        rows = cursor.fetchall()

        if rows:
            print("margin_rebate sample:")
            print(f"{'Customer':<15} {'ID':<8} {'Amount':<15}")
            print("-" * 40)
            for row in rows:
                print(f"{row[0]:<15} {row[1] or 'N/A':<8} ${row[2]:>12,.2f}")
            print(f"\n✅ Client Gold views working!")
        else:
            print("Client Gold views created but no data yet.")

    except pymssql.Error as e:
        print(f"Client Gold views not populated yet: {e}")

    conn.close()

    # Print summary
    print("\n" + "=" * 60)
    print("SETUP SUMMARY")
    print("=" * 60)
    print(f"""
Synapse Server:  {CONFIG['server']}
Database:        {CONFIG['database']}
Data Source:     TrinityLake
Credential:      TrinityStorageKey

Views Created:

  ERP Pipeline:
  - sales_by_region   (Gold - regional aggregates)
  - sales_by_product  (Gold - product aggregates)
  - silver_sales      (Silver - cleaned sales data)

  Client Pipeline:
  - fact_invoice          (Gold - invoice line items with margins)
  - fact_invoice_detail   (Gold - invoice to customer mapping)
  - fact_order_item       (Gold - order details)
  - dim_customer          (Gold - customer dimension)
  - margin_invoice        (Gold - denormalized reporting table)
  - fact_general_ledger   (Gold - general ledger with rebate info)
  - margin_rebate         (Gold - rebates aggregated by customer)

To query in Synapse Studio or Power BI:
1. Select database: {CONFIG['database']} (NOT master)
2. Query the views directly:

-- Client Gold layer (main reporting tables)
SELECT * FROM margin_invoice;      -- Invoice margins by customer
SELECT * FROM margin_rebate;       -- Rebates by customer
SELECT * FROM fact_invoice;        -- Invoice details
SELECT * FROM dim_customer;        -- Customer dimension

-- ERP Gold layer
SELECT * FROM sales_by_region;
SELECT * FROM sales_by_product;

Power BI Connection:
- Server: {CONFIG['server']}
- Database: {CONFIG['database']}
- Use SQL Server authentication or Azure AD
""")


if __name__ == "__main__":
    setup_synapse()
