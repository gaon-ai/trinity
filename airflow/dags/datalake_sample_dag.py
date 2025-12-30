"""
Sample DAG that writes data to Azure Data Lake Storage Gen2 and Azure SQL.

This DAG demonstrates:
1. Generating sample sales data
2. Writing to Bronze layer (raw)
3. Transforming and writing to Silver layer (cleaned)
4. Aggregating and writing to Gold layer (business-ready)
5. Loading Gold aggregations to Azure SQL for Power BI
"""
import os
import json
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.storage.filedatalake import DataLakeServiceClient


# Data Lake configuration from environment
STORAGE_ACCOUNT_NAME = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME', '')
STORAGE_ACCOUNT_KEY = os.environ.get('AZURE_STORAGE_ACCOUNT_KEY', '')

# Azure SQL configuration from environment
SQL_SERVER = os.environ.get('AZURE_SQL_SERVER', '')
SQL_DATABASE = os.environ.get('AZURE_SQL_DATABASE', 'trinity')
SQL_USER = os.environ.get('AZURE_SQL_USER', 'sqladmin')
SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD', '')


def get_datalake_client():
    """Create Data Lake service client."""
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)


def write_to_datalake(container: str, path: str, data: str):
    """Write data to Data Lake."""
    service_client = get_datalake_client()
    file_system_client = service_client.get_file_system_client(container)
    file_client = file_system_client.get_file_client(path)
    file_client.upload_data(data, overwrite=True)
    print(f"Wrote data to {container}/{path}")


def get_date_hour(context) -> str:
    """Get execution date in YYYY-MM-DD-HH format."""
    # Use logical_date (Airflow 2.2+) or execution_date
    dt = context.get('logical_date') or context.get('execution_date')
    return dt.strftime('%Y-%m-%d-%H')


def generate_sample_data(**context):
    """Generate sample sales data and write to Bronze layer."""
    date_hour = get_date_hour(context)
    execution_date = context['ds']  # For data content

    # Sample sales data
    data = {
        'order_id': [1001, 1002, 1003, 1004, 1005],
        'customer_id': ['C001', 'C002', 'C001', 'C003', 'C002'],
        'product': ['Widget A', 'Widget B', 'Widget A', 'Widget C', 'Widget B'],
        'quantity': [2, 1, 3, 1, 2],
        'unit_price': [29.99, 49.99, 29.99, 79.99, 49.99],
        'order_date': [execution_date] * 5,
        'region': ['East', 'West', 'East', 'North', 'West'],
    }

    df = pd.DataFrame(data)

    # Write raw JSON to Bronze layer
    # Path: bronze/{source}/{dataset}/{YYYY-MM-DD-HH}/raw.json
    json_data = df.to_json(orient='records', indent=2)
    bronze_path = f"erp/sales/{date_hour}/raw.json"
    write_to_datalake('bronze', bronze_path, json_data)

    # Return path for next task
    return bronze_path


def transform_to_silver(**context):
    """Read from Bronze, clean/transform, write to Silver layer."""
    date_hour = get_date_hour(context)
    execution_date = context['ds']  # For data content

    # In a real scenario, you'd read from Bronze
    # For this example, we'll recreate and transform the data
    data = {
        'order_id': [1001, 1002, 1003, 1004, 1005],
        'customer_id': ['C001', 'C002', 'C001', 'C003', 'C002'],
        'product': ['Widget A', 'Widget B', 'Widget A', 'Widget C', 'Widget B'],
        'quantity': [2, 1, 3, 1, 2],
        'unit_price': [29.99, 49.99, 29.99, 79.99, 49.99],
        'order_date': [execution_date] * 5,
        'region': ['East', 'West', 'East', 'North', 'West'],
    }

    df = pd.DataFrame(data)

    # Transform: Add calculated fields
    df['total_amount'] = df['quantity'] * df['unit_price']
    df['processed_at'] = datetime.now().isoformat()

    # Write cleaned CSV to Silver layer
    # Path: silver/{dataset}/{YYYY-MM-DD-HH}/cleaned.csv
    csv_data = df.to_csv(index=False)
    silver_path = f"sales/{date_hour}/cleaned.csv"
    write_to_datalake('silver', silver_path, csv_data)

    return silver_path


def aggregate_to_gold(**context):
    """Aggregate data and write to Gold layer for BI consumption."""
    date_hour = get_date_hour(context)
    execution_date = context['ds']  # For data content

    # Sample transformed data
    data = {
        'order_id': [1001, 1002, 1003, 1004, 1005],
        'customer_id': ['C001', 'C002', 'C001', 'C003', 'C002'],
        'product': ['Widget A', 'Widget B', 'Widget A', 'Widget C', 'Widget B'],
        'quantity': [2, 1, 3, 1, 2],
        'unit_price': [29.99, 49.99, 29.99, 79.99, 49.99],
        'region': ['East', 'West', 'East', 'North', 'West'],
    }

    df = pd.DataFrame(data)
    df['total_amount'] = df['quantity'] * df['unit_price']

    # Aggregate: Sales by region
    region_summary = df.groupby('region').agg({
        'order_id': 'count',
        'quantity': 'sum',
        'total_amount': 'sum'
    }).reset_index()
    region_summary.columns = ['region', 'order_count', 'total_quantity', 'total_revenue']
    region_summary['report_date'] = execution_date

    # Aggregate: Sales by product
    product_summary = df.groupby('product').agg({
        'order_id': 'count',
        'quantity': 'sum',
        'total_amount': 'sum'
    }).reset_index()
    product_summary.columns = ['product', 'order_count', 'total_quantity', 'total_revenue']
    product_summary['report_date'] = execution_date

    # Write aggregated data to Gold layer
    # Path: gold/{dataset}/{YYYY-MM-DD-HH}/data.csv
    region_csv = region_summary.to_csv(index=False)
    write_to_datalake('gold', f"sales_by_region/{date_hour}/data.csv", region_csv)

    product_csv = product_summary.to_csv(index=False)
    write_to_datalake('gold', f"sales_by_product/{date_hour}/data.csv", product_csv)

    # Write a summary for serving layer (could be loaded to Azure SQL)
    summary = {
        'report_date': execution_date,
        'total_orders': len(df),
        'total_revenue': float(df['total_amount'].sum()),
        'regions': region_summary.to_dict(orient='records'),
        'products': product_summary.to_dict(orient='records'),
        'generated_at': datetime.now().isoformat()
    }

    summary_json = json.dumps(summary, indent=2)
    write_to_datalake('gold', f"daily_summary/{date_hour}/summary.json", summary_json)

    print(f"Gold layer aggregations complete for {execution_date}")
    print(f"Total Revenue: ${summary['total_revenue']:.2f}")

    return summary


def load_to_sql(**context):
    """Load Gold layer aggregations to Azure SQL for Power BI consumption."""
    import pymssql

    execution_date = context['ds']

    if not SQL_SERVER or not SQL_PASSWORD:
        print("Azure SQL credentials not configured. Skipping SQL load.")
        print("Set AZURE_SQL_SERVER and AZURE_SQL_PASSWORD in .env")
        return None

    # Sample transformed data (in production, read from Gold layer)
    data = {
        'order_id': [1001, 1002, 1003, 1004, 1005],
        'customer_id': ['C001', 'C002', 'C001', 'C003', 'C002'],
        'product': ['Widget A', 'Widget B', 'Widget A', 'Widget C', 'Widget B'],
        'quantity': [2, 1, 3, 1, 2],
        'unit_price': [29.99, 49.99, 29.99, 79.99, 49.99],
        'region': ['East', 'West', 'East', 'North', 'West'],
    }

    df = pd.DataFrame(data)
    df['total_amount'] = df['quantity'] * df['unit_price']

    # Aggregate by region
    region_summary = df.groupby('region').agg({
        'order_id': 'count',
        'quantity': 'sum',
        'total_amount': 'sum'
    }).reset_index()
    region_summary.columns = ['region', 'order_count', 'total_quantity', 'total_revenue']
    region_summary['report_date'] = execution_date

    # Aggregate by product
    product_summary = df.groupby('product').agg({
        'order_id': 'count',
        'quantity': 'sum',
        'total_amount': 'sum'
    }).reset_index()
    product_summary.columns = ['product', 'order_count', 'total_quantity', 'total_revenue']
    product_summary['report_date'] = execution_date

    # Connect to Azure SQL
    conn = pymssql.connect(
        server=SQL_SERVER,
        user=SQL_USER,
        password=SQL_PASSWORD,
        database=SQL_DATABASE
    )
    cursor = conn.cursor()

    # Create tables if not exist
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sales_by_region')
        CREATE TABLE sales_by_region (
            id INT IDENTITY(1,1) PRIMARY KEY,
            region NVARCHAR(50),
            order_count INT,
            total_quantity INT,
            total_revenue DECIMAL(18,2),
            report_date DATE,
            loaded_at DATETIME DEFAULT GETDATE()
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sales_by_product')
        CREATE TABLE sales_by_product (
            id INT IDENTITY(1,1) PRIMARY KEY,
            product NVARCHAR(100),
            order_count INT,
            total_quantity INT,
            total_revenue DECIMAL(18,2),
            report_date DATE,
            loaded_at DATETIME DEFAULT GETDATE()
        )
    """)

    # Delete existing data for this date (idempotent)
    cursor.execute("DELETE FROM sales_by_region WHERE report_date = %s", (execution_date,))
    cursor.execute("DELETE FROM sales_by_product WHERE report_date = %s", (execution_date,))

    # Insert region summary
    for _, row in region_summary.iterrows():
        cursor.execute(
            "INSERT INTO sales_by_region (region, order_count, total_quantity, total_revenue, report_date) VALUES (%s, %s, %s, %s, %s)",
            (row['region'], row['order_count'], row['total_quantity'], row['total_revenue'], row['report_date'])
        )

    # Insert product summary
    for _, row in product_summary.iterrows():
        cursor.execute(
            "INSERT INTO sales_by_product (product, order_count, total_quantity, total_revenue, report_date) VALUES (%s, %s, %s, %s, %s)",
            (row['product'], row['order_count'], row['total_quantity'], row['total_revenue'], row['report_date'])
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(region_summary)} region records and {len(product_summary)} product records to Azure SQL")
    return {'regions': len(region_summary), 'products': len(product_summary)}


default_args = {
    'owner': 'trinity',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'datalake_sample',
    default_args=default_args,
    description='Sample DAG that writes to Azure Data Lake (Bronze → Silver → Gold → SQL)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['datalake', 'sample', 'medallion', 'azure-sql'],
) as dag:

    # Task 1: Generate sample data and write to Bronze
    bronze_task = PythonOperator(
        task_id='write_to_bronze',
        python_callable=generate_sample_data,
    )

    # Task 2: Transform and write to Silver
    silver_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
    )

    # Task 3: Aggregate and write to Gold
    gold_task = PythonOperator(
        task_id='aggregate_to_gold',
        python_callable=aggregate_to_gold,
    )

    # Task 4: Load Gold aggregations to Azure SQL for Power BI
    sql_task = PythonOperator(
        task_id='load_to_sql',
        python_callable=load_to_sql,
    )

    # Pipeline: Bronze → Silver → Gold → SQL
    bronze_task >> silver_task >> gold_task >> sql_task
