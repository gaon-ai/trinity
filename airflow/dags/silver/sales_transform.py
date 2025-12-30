"""
Silver Layer: Sales Data Transformation

Reads raw sales from Bronze, cleans and transforms, writes to Silver.
Triggered by BRONZE_SALES dataset completion.
Publishes to SILVER_SALES dataset to trigger Gold aggregation.

Schedule: On Bronze dataset update
Path: silver/sales/{YYYY-MM-DD-HH}/cleaned.csv
"""
import json
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.datalake import read_from_datalake, write_to_datalake, write_metadata, wait_for_file
from lib.utils import get_date_hour, build_bronze_path, build_silver_path
from lib.datasets import BRONZE_SALES, SILVER_SALES

# Configuration
WAIT_TIMEOUT_SECONDS = 300  # 5 minutes
WAIT_POLL_INTERVAL = 10     # Check every 10 seconds


def transform_sales(**context):
    """
    Read raw sales from Bronze, transform, write to Silver.

    Transformations:
    - Calculate total_amount
    - Add processed_at timestamp
    - Data quality validation
    """
    partition = get_date_hour(context)
    execution_date = context['ds']

    # Wait for Bronze data to be ready
    bronze_path = build_bronze_path('erp', 'sales', partition, 'raw.json')
    wait_for_file(
        'bronze', bronze_path,
        timeout_seconds=WAIT_TIMEOUT_SECONDS,
        poll_interval=WAIT_POLL_INTERVAL,
        raise_on_timeout=True
    )

    # Read from Bronze
    raw_json = read_from_datalake('bronze', bronze_path)
    df = pd.DataFrame(json.loads(raw_json))
    print(f"Read {len(df)} records from Bronze: {bronze_path}")

    # Transform
    df['total_amount'] = df['quantity'] * df['unit_price']
    df['processed_at'] = datetime.now().isoformat()

    # Data quality checks
    assert df['quantity'].min() > 0, "Quantity must be positive"
    assert df['unit_price'].min() > 0, "Unit price must be positive"

    # Write to Silver
    csv_data = df.to_csv(index=False)
    silver_path = build_silver_path('sales', partition, 'cleaned.csv')
    write_to_datalake('silver', silver_path, csv_data)

    # Lineage metadata
    write_metadata('silver', silver_path,
        source_layer='bronze',
        source_path=f"bronze/{bronze_path}",
        extra={
            'partition': partition,
            'record_count': len(df),
            'transformations': ['calculate_total_amount', 'add_processed_at'],
        }
    )

    print(f"Silver transform complete: {len(df)} records → {silver_path}")
    return silver_path


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'silver_sales_transform',
    default_args=default_args,
    description='Silver: Transform sales data',
    schedule=[BRONZE_SALES],  # Triggered by Bronze
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver', 'sales', 'medallion'],
) as dag:

    transform = PythonOperator(
        task_id='transform_sales',
        python_callable=transform_sales,
        outlets=[SILVER_SALES],
    )
