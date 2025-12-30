"""
Gold Layer: Sales Aggregations

Reads cleaned sales from Silver, creates business aggregates.
Triggered by SILVER_SALES dataset completion.
Query results via Synapse Serverless views.

Schedule: On Silver dataset update
Paths:
  - gold/sales_by_region/{YYYY-MM-DD-HH}/data.csv
  - gold/sales_by_product/{YYYY-MM-DD-HH}/data.csv
  - gold/daily_summary/{YYYY-MM-DD-HH}/summary.json
"""
import json
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.datalake import read_from_datalake, write_to_datalake, write_metadata, wait_for_file
from lib.utils import get_date_hour, get_execution_date, build_silver_path, build_gold_path
from lib.datasets import SILVER_SALES, GOLD_SALES_BY_REGION, GOLD_SALES_BY_PRODUCT

# Configuration
WAIT_TIMEOUT_SECONDS = 300  # 5 minutes
WAIT_POLL_INTERVAL = 10     # Check every 10 seconds


def load_silver_sales(partition: str) -> pd.DataFrame:
    """Load sales data from Silver layer with wait."""
    silver_path = build_silver_path('sales', partition, 'cleaned.csv')

    # Wait for Silver data to be ready
    wait_for_file(
        'silver', silver_path,
        timeout_seconds=WAIT_TIMEOUT_SECONDS,
        poll_interval=WAIT_POLL_INTERVAL,
        raise_on_timeout=True
    )

    # Read from Silver
    csv_data = read_from_datalake('silver', silver_path)
    df = pd.read_csv(StringIO(csv_data))
    print(f"Read {len(df)} records from Silver")
    return df


def aggregate_by_region(**context):
    """Create sales by region aggregation."""
    partition = get_date_hour(context)
    execution_date = get_execution_date(context)
    df = load_silver_sales(partition)

    # Aggregate
    summary = df.groupby('region').agg({
        'order_id': 'count',
        'quantity': 'sum',
        'total_amount': 'sum'
    }).reset_index()
    summary.columns = ['region', 'order_count', 'total_quantity', 'total_revenue']
    summary['report_date'] = execution_date

    # Write
    gold_path = build_gold_path('sales_by_region', partition, 'data.csv')
    write_to_datalake('gold', gold_path, summary.to_csv(index=False))
    write_metadata('gold', gold_path,
        source_layer='silver',
        source_path=f"silver/sales/{partition}/cleaned.csv",
        extra={'aggregation': 'by_region', 'record_count': len(summary)}
    )

    print(f"Gold (by region): {len(summary)} regions → {gold_path}")
    return gold_path


def aggregate_by_product(**context):
    """Create sales by product aggregation."""
    partition = get_date_hour(context)
    execution_date = get_execution_date(context)
    df = load_silver_sales(partition)

    # Aggregate
    summary = df.groupby('product').agg({
        'order_id': 'count',
        'quantity': 'sum',
        'total_amount': 'sum'
    }).reset_index()
    summary.columns = ['product', 'order_count', 'total_quantity', 'total_revenue']
    summary['report_date'] = execution_date

    # Write
    gold_path = build_gold_path('sales_by_product', partition, 'data.csv')
    write_to_datalake('gold', gold_path, summary.to_csv(index=False))
    write_metadata('gold', gold_path,
        source_layer='silver',
        source_path=f"silver/sales/{partition}/cleaned.csv",
        extra={'aggregation': 'by_product', 'record_count': len(summary)}
    )

    print(f"Gold (by product): {len(summary)} products → {gold_path}")
    return gold_path


def create_daily_summary(**context):
    """Create daily summary JSON."""
    partition = get_date_hour(context)
    execution_date = get_execution_date(context)
    df = load_silver_sales(partition)

    summary = {
        'report_date': execution_date,
        'partition': partition,
        'total_orders': len(df),
        'total_revenue': float(df['total_amount'].sum()),
        'avg_order_value': float(df['total_amount'].mean()),
        'by_region': df.groupby('region')['total_amount'].sum().to_dict(),
        'by_product': df.groupby('product')['total_amount'].sum().to_dict(),
        'generated_at': datetime.now().isoformat(),
    }

    gold_path = build_gold_path('daily_summary', partition, 'summary.json')
    write_to_datalake('gold', gold_path, json.dumps(summary, indent=2))

    print(f"Daily summary: ${summary['total_revenue']:.2f} revenue → {gold_path}")
    return summary


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gold_sales_aggregation',
    default_args=default_args,
    description='Gold: Aggregate sales for BI',
    schedule=[SILVER_SALES],  # Triggered by Silver
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold', 'sales', 'medallion'],
) as dag:

    # These run in parallel
    by_region = PythonOperator(
        task_id='aggregate_by_region',
        python_callable=aggregate_by_region,
        outlets=[GOLD_SALES_BY_REGION],
    )

    by_product = PythonOperator(
        task_id='aggregate_by_product',
        python_callable=aggregate_by_product,
        outlets=[GOLD_SALES_BY_PRODUCT],
    )

    daily = PythonOperator(
        task_id='create_daily_summary',
        python_callable=create_daily_summary,
    )

    # All aggregations run in parallel (no dependencies)
    [by_region, by_product, daily]
