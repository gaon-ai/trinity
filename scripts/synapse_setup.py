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

    # Test query
    print("\n" + "=" * 60)
    print("Testing views...")
    print("=" * 60 + "\n")

    try:
        cursor.execute("SELECT TOP 5 * FROM sales_by_region")
        rows = cursor.fetchall()

        if rows:
            print("sales_by_region sample:")
            print(f"{'region':<10} {'orders':<8} {'qty':<8} {'revenue':<12} {'date':<12}")
            print("-" * 50)
            for row in rows:
                print(f"{row[0]:<10} {row[1]:<8} {row[2]:<8} ${row[3]:<11.2f} {row[4]:<12}")
            print(f"\n✅ Views created and working!")
        else:
            print("Views created but no data yet. Run the Airflow DAG first.")

    except pymssql.Error as e:
        print(f"Query error: {e}")
        print("\nViews created but test failed. Run the Airflow DAG to populate data.")

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
- sales_by_region   (Gold layer - regional aggregates)
- sales_by_product  (Gold layer - product aggregates)
- silver_sales      (Silver layer - cleaned sales data)

To query in Synapse Studio or Power BI:
1. Select database: {CONFIG['database']} (NOT master)
2. Query the views directly:

-- Gold layer aggregates
SELECT * FROM sales_by_region;
SELECT * FROM sales_by_product;

-- Silver layer detail
SELECT * FROM silver_sales;

Power BI Connection:
- Server: {CONFIG['server']}
- Database: {CONFIG['database']}
- Use SQL Server authentication or Azure AD
""")


if __name__ == "__main__":
    setup_synapse()
