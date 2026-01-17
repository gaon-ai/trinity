"""
Gold Layer: Client Fact and Dimension Tables

Creates business-ready fact and dimension tables from bronze client data.

Schedule: Triggered when bronze client inventory ingestion completes
Path: gold/{table_name}/{YYYY-MM-DD-HH}/data.csv

Tables:
- fact_invoice: Invoice line items with calculated margins
- fact_invoice_detail: Invoice to customer mapping
- fact_order_item: Order details
- dim_customer: Customer dimension with normalized names
- margin_invoice: Denormalized reporting table (joins all)
- fact_general_ledger: General ledger with rebate customer extraction
- margin_rebate: Rebate aggregation by customer

Manual Trigger:
    airflow dags trigger gold_client_facts
"""
import json
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.datalake import read_from_datalake, write_to_datalake, write_metadata, wait_for_file
from lib.utils import get_date_hour, build_gold_path
from lib.transformations.client import (
    transform_fact_invoice,
    transform_fact_invoice_detail,
    transform_fact_order_item,
    transform_dim_customer,
    create_margin_invoice,
    transform_fact_general_ledger,
    create_margin_rebate,
)
from lib.datasets import (
    BRONZE_CLIENT_DATA,
    GOLD_FACT_INVOICE,
    GOLD_FACT_INVOICE_DETAIL,
    GOLD_FACT_ORDER_ITEM,
    GOLD_DIM_CUSTOMER,
    GOLD_MARGIN_INVOICE,
    GOLD_FACT_GENERAL_LEDGER,
    GOLD_MARGIN_REBATE,
)


# Bronze table paths
BRONZE_TABLES = {
    'invoice_item': 'client/invoice_item',
    'invoice_header': 'client/invoice_header',
    'order_item': 'client/order_item',
    'customer_address': 'client/customer_address',
    'general_ledger': 'client/general_ledger',
}

# Wait configuration
WAIT_TIMEOUT_SECONDS = 300
WAIT_POLL_INTERVAL = 10


def _find_latest_partition(container: str, base_path: str) -> str:
    """Find the latest partition directory (YYYY-MM-DD-HH format)."""
    from azure.storage.filedatalake import DataLakeServiceClient
    from lib.datalake import get_datalake_client

    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(container)

    # List directories under base_path
    partitions = []
    try:
        paths = fs_client.get_paths(path=base_path)
        for path_item in paths:
            # Extract partition from path (e.g., client/invoice_item/2025-01-17-10)
            parts = path_item.name.split('/')
            if len(parts) >= 3 and path_item.is_directory:
                partition = parts[-1]
                # Validate it looks like a date partition
                if len(partition) == 13 and partition[4] == '-':
                    partitions.append(partition)
    except Exception as e:
        print(f"Error listing partitions: {e}")
        return None

    if not partitions:
        return None

    # Return latest partition (lexicographic sort works for YYYY-MM-DD-HH)
    return sorted(partitions)[-1]


def _load_bronze_table(table_name: str, partition: str = None) -> pd.DataFrame:
    """Load a bronze table from Data Lake."""
    base_path = BRONZE_TABLES[table_name]

    # Find latest partition if not specified
    if partition is None:
        partition = _find_latest_partition('bronze', base_path)
        if partition is None:
            raise ValueError(f"No partitions found for {table_name}")

    path = f"{base_path}/{partition}/data.json"
    print(f"  Loading {table_name} from bronze/{path}")

    # Wait for file to be available
    wait_for_file(
        'bronze', path,
        timeout_seconds=WAIT_TIMEOUT_SECONDS,
        poll_interval=WAIT_POLL_INTERVAL,
        raise_on_timeout=True
    )

    # Read JSON data
    json_data = read_from_datalake('bronze', path)
    data = json.loads(json_data)
    df = pd.DataFrame(data)
    print(f"  Loaded {len(df)} records from {table_name}")
    return df, partition


def _write_gold_table(df: pd.DataFrame, table_name: str, partition: str, source_tables: list):
    """Write a gold table to Data Lake."""
    gold_path = build_gold_path(table_name, partition, 'data.csv')
    csv_data = df.to_csv(index=False)
    write_to_datalake('gold', gold_path, csv_data)

    # Write metadata
    write_metadata('gold', gold_path,
        source_layer='bronze',
        source_path=f"bronze/client/*/",
        extra={
            'table': table_name,
            'record_count': len(df),
            'source_tables': source_tables,
            'partition': partition,
        }
    )
    print(f"  Wrote {len(df)} records to gold/{gold_path}")
    return gold_path


def create_fact_invoice(**context):
    """Create fact_invoice from invoice_item."""
    print("Creating fact_invoice...")
    df, partition = _load_bronze_table('invoice_item')
    result = transform_fact_invoice(df)
    path = _write_gold_table(result, 'fact_invoice', partition, ['invoice_item'])
    return {'table': 'fact_invoice', 'records': len(result), 'path': path}


def create_fact_invoice_detail(**context):
    """Create fact_invoice_detail from invoice_header."""
    print("Creating fact_invoice_detail...")
    df, partition = _load_bronze_table('invoice_header')
    result = transform_fact_invoice_detail(df)
    path = _write_gold_table(result, 'fact_invoice_detail', partition, ['invoice_header'])
    return {'table': 'fact_invoice_detail', 'records': len(result), 'path': path}


def create_fact_order_item(**context):
    """Create fact_order_item from order_item."""
    print("Creating fact_order_item...")
    df, partition = _load_bronze_table('order_item')
    result = transform_fact_order_item(df)
    path = _write_gold_table(result, 'fact_order_item', partition, ['order_item'])
    return {'table': 'fact_order_item', 'records': len(result), 'path': path}


def create_dim_customer(**context):
    """Create dim_customer from customer_address."""
    print("Creating dim_customer...")
    df, partition = _load_bronze_table('customer_address')
    result = transform_dim_customer(df)
    path = _write_gold_table(result, 'dim_customer', partition, ['customer_address'])
    return {'table': 'dim_customer', 'records': len(result), 'path': path}


def create_margin_invoice_table(**context):
    """Create margin_invoice by joining all tables."""
    print("Creating margin_invoice (joining all tables)...")

    # Load all bronze tables
    invoice_item, partition = _load_bronze_table('invoice_item')
    invoice_header, _ = _load_bronze_table('invoice_header', partition)
    order_item, _ = _load_bronze_table('order_item', partition)
    customer_address, _ = _load_bronze_table('customer_address', partition)

    # Create joined table
    result = create_margin_invoice(
        invoice_item=invoice_item,
        invoice_header=invoice_header,
        order_item=order_item,
        customer_address=customer_address
    )

    path = _write_gold_table(
        result, 'margin_invoice', partition,
        ['invoice_item', 'invoice_header', 'order_item', 'customer_address']
    )
    return {'table': 'margin_invoice', 'records': len(result), 'path': path}


def create_fact_general_ledger(**context):
    """Create fact_general_ledger from general_ledger."""
    print("Creating fact_general_ledger...")
    df, partition = _load_bronze_table('general_ledger')
    result = transform_fact_general_ledger(df)
    path = _write_gold_table(result, 'fact_general_ledger', partition, ['general_ledger'])
    return {'table': 'fact_general_ledger', 'records': len(result), 'path': path}


def create_margin_rebate_table(**context):
    """Create margin_rebate by aggregating from fact_general_ledger."""
    print("Creating margin_rebate...")

    # First create fact_general_ledger
    df, partition = _load_bronze_table('general_ledger')
    fact_gl = transform_fact_general_ledger(df)

    # Then aggregate to margin_rebate
    result = create_margin_rebate(fact_gl)
    path = _write_gold_table(result, 'margin_rebate', partition, ['general_ledger'])
    return {'table': 'margin_rebate', 'records': len(result), 'path': path}


# =============================================================================
# DAG DEFINITION
# =============================================================================
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='gold_client_facts',
    default_args=default_args,
    description='Gold: Create fact and dimension tables from client data',
    schedule=[BRONZE_CLIENT_DATA],  # Triggered by bronze completion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold', 'client', 'facts', 'dimensions', 'medallion'],
    doc_md=__doc__,
) as dag:

    # Individual fact/dimension tables (run in parallel)
    fact_invoice = PythonOperator(
        task_id='create_fact_invoice',
        python_callable=create_fact_invoice,
        outlets=[GOLD_FACT_INVOICE],
    )

    fact_invoice_detail = PythonOperator(
        task_id='create_fact_invoice_detail',
        python_callable=create_fact_invoice_detail,
        outlets=[GOLD_FACT_INVOICE_DETAIL],
    )

    fact_order_item = PythonOperator(
        task_id='create_fact_order_item',
        python_callable=create_fact_order_item,
        outlets=[GOLD_FACT_ORDER_ITEM],
    )

    dim_customer = PythonOperator(
        task_id='create_dim_customer',
        python_callable=create_dim_customer,
        outlets=[GOLD_DIM_CUSTOMER],
    )

    # Margin invoice depends on all other tables being created first
    margin_invoice = PythonOperator(
        task_id='create_margin_invoice',
        python_callable=create_margin_invoice_table,
        outlets=[GOLD_MARGIN_INVOICE],
    )

    # General ledger tables (independent of invoice tables)
    fact_general_ledger = PythonOperator(
        task_id='create_fact_general_ledger',
        python_callable=create_fact_general_ledger,
        outlets=[GOLD_FACT_GENERAL_LEDGER],
    )

    margin_rebate = PythonOperator(
        task_id='create_margin_rebate',
        python_callable=create_margin_rebate_table,
        outlets=[GOLD_MARGIN_REBATE],
    )

    # Task dependencies:
    # - Invoice-related facts run in parallel, then margin_invoice
    # - General ledger facts run in parallel, then margin_rebate
    [fact_invoice, fact_invoice_detail, fact_order_item, dim_customer] >> margin_invoice
    fact_general_ledger >> margin_rebate
