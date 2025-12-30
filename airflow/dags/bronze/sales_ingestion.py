"""
Bronze Layer: Sales Data Ingestion

Ingests raw sales data from sources and writes to Bronze layer.
Publishes to BRONZE_SALES dataset to trigger Silver transform.

Schedule: @hourly
Path: bronze/erp/sales/{YYYY-MM-DD-HH}/raw.json
"""
import json
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.datalake import write_to_datalake, write_metadata
from lib.utils import get_date_hour, build_bronze_path
from lib.datasets import BRONZE_SALES


def ingest_sales(**context):
    """
    Ingest sales data from source and write to Bronze layer.

    In production, this would:
    - Pull from API, database, or file drop
    - Write raw data without transformation
    """
    partition = get_date_hour(context)
    execution_date = context['ds']

    # Simulated raw data from ERP system
    # TODO: Replace with actual data source
    raw_data = {
        'order_id': [1001, 1002, 1003, 1004, 1005],
        'customer_id': ['C001', 'C002', 'C001', 'C003', 'C002'],
        'product': ['Widget A', 'Widget B', 'Widget A', 'Widget C', 'Widget B'],
        'quantity': [2, 1, 3, 1, 2],
        'unit_price': [29.99, 49.99, 29.99, 79.99, 49.99],
        'order_date': [execution_date] * 5,
        'region': ['East', 'West', 'East', 'North', 'West'],
    }

    df = pd.DataFrame(raw_data)

    # Write raw JSON to Bronze layer
    json_data = df.to_json(orient='records', indent=2)
    path = build_bronze_path('erp', 'sales', partition, 'raw.json')
    write_to_datalake('bronze', path, json_data)

    # Write lineage metadata
    write_metadata('bronze', path, extra={
        'source': 'erp_system',
        'dataset': 'sales',
        'partition': partition,
        'record_count': len(df),
        'schema_version': '1.0',
    })

    print(f"Bronze ingestion complete: {len(df)} records → {path}")
    return path


default_args = {
    'owner': 'trinity',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bronze_sales_ingestion',
    default_args=default_args,
    description='Bronze: Ingest raw sales data',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'sales', 'medallion'],
) as dag:

    ingest = PythonOperator(
        task_id='ingest_sales',
        python_callable=ingest_sales,
        outlets=[BRONZE_SALES],
    )
