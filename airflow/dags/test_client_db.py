"""
Test DAG: Verify Client SQL Server Connectivity

This DAG tests the connection to the client's on-prem SQL Server
accessible via site-to-site VPN.

Manual trigger only - used to verify connectivity.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def test_client_db_connection(**context):
    """
    Test connection to client SQL Server.

    Performs:
    1. Basic connectivity test
    2. Query sample table (inv_item_mst)
    3. Display sample data and schema info
    """
    from lib.client_db import (
        test_connection,
        fetch_query,
        get_table_schema,
        get_row_count,
        get_connection_info,
    )

    conn_info = get_connection_info()

    print("=" * 60)
    print("CLIENT SQL SERVER CONNECTION TEST")
    print("=" * 60)
    print(f"Server: {conn_info['server']}")
    print(f"Database: {conn_info['database']}")
    print(f"User: {conn_info['user']}")
    print("(Credentials from Airflow Connection - encrypted)")
    print("=" * 60)

    # Test 1: Basic connectivity
    print("\n[Test 1] Testing basic connectivity...")
    if test_connection():
        print("SUCCESS: Connected to client SQL Server")
    else:
        raise Exception("FAILED: Could not connect to client SQL Server")

    # Test 2: Get table schema
    print("\n[Test 2] Getting schema for inv_item_mst...")
    schema = get_table_schema('inv_item_mst')
    print(f"SUCCESS: Found {len(schema)} columns")
    print("First 10 columns:")
    for col in schema[:10]:
        nullable = "NULL" if col['IS_NULLABLE'] == 'YES' else "NOT NULL"
        print(f"  - {col['COLUMN_NAME']}: {col['DATA_TYPE']} ({nullable})")

    # Test 3: Get row count
    print("\n[Test 3] Getting row count...")
    row_count = get_row_count('inv_item_mst')
    print(f"SUCCESS: inv_item_mst has approximately {row_count:,} rows")

    # Test 4: Query sample data
    print("\n[Test 4] Querying sample data...")
    results = fetch_query("SELECT TOP 5 * FROM [dbo].[inv_item_mst]")
    print(f"SUCCESS: Retrieved {len(results)} rows")

    if results:
        print("\nSample row keys:", list(results[0].keys())[:10])
        print("\nFirst row preview:")
        first_row = results[0]
        for i, (key, value) in enumerate(first_row.items()):
            if i >= 10:
                print("  ... (truncated)")
                break
            print(f"  {key}: {value}")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED - Client DB connection is working!")
    print("=" * 60)

    return "Connection test successful!"


with DAG(
    dag_id='test_client_db',
    description='Test client SQL Server connectivity via VPN',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test', 'client-db', 'vpn'],
    doc_md="""
    ## Test Client Database Connection

    This DAG verifies connectivity to the client's on-prem SQL Server
    accessible via site-to-site VPN.

    **Prerequisites:**
    - Site-to-site VPN configured and active
    - `AIRFLOW_CONN_CLIENT_SQLSERVER` environment variable set in .env

    **Tests Performed:**
    1. Basic connectivity
    2. Table schema retrieval
    3. Row count query
    4. Sample data query

    **Usage:**
    Trigger manually to verify VPN and database connectivity.
    """,
) as dag:

    test_connection_task = PythonOperator(
        task_id='test_connection',
        python_callable=test_client_db_connection,
    )
