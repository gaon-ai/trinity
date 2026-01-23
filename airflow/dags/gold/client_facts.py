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
- dim_lessee: Lessee dimension with contract summary
- margin_invoice: Denormalized reporting table (joins all)
- fact_general_ledger: General ledger with rebate customer extraction
- margin_rebate: Rebate aggregation by customer

Manual Trigger:
    airflow dags trigger gold_client_facts
"""
import json
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.datalake import read_from_datalake, write_to_datalake, write_metadata, wait_for_file
from lib.utils import build_gold_path
from lib.transformations.client import (
    transform_fact_invoice,
    transform_fact_invoice_detail,
    transform_fact_order_item,
    transform_dim_customer,
    create_dim_lessee,
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
    GOLD_DIM_LESSEE,
    GOLD_MARGIN_INVOICE,
    GOLD_FACT_GENERAL_LEDGER,
    GOLD_MARGIN_REBATE,
)
from lib.synapse import create_gold_views


# =============================================================================
# CONFIGURATION
# =============================================================================

BRONZE_TABLES = {
    'invoice_item': 'client/invoice_item',
    'invoice_header': 'client/invoice_header',
    'order_item': 'client/order_item',
    'customer_address': 'client/customer_address',
    'general_ledger': 'client/general_ledger',
    'report_lessee': 'client/report_lessee',
    'report_contract': 'client/report_contract',
}

WAIT_TIMEOUT_SECONDS = 300
WAIT_POLL_INTERVAL = 10


# =============================================================================
# DATA LAKE HELPERS
# =============================================================================

def _find_latest_partition(container: str, base_path: str) -> str:
    """Find the latest partition directory (YYYY-MM-DD-HH format)."""
    from lib.datalake import get_datalake_client

    service_client = get_datalake_client()
    fs_client = service_client.get_file_system_client(container)

    partitions = []
    try:
        paths = fs_client.get_paths(path=base_path)
        for path_item in paths:
            parts = path_item.name.split('/')
            if len(parts) >= 3 and path_item.is_directory:
                partition = parts[-1]
                if len(partition) == 13 and partition[4] == '-':
                    partitions.append(partition)
    except Exception as e:
        print(f"Error listing partitions: {e}")
        return None

    return sorted(partitions)[-1] if partitions else None


def _load_bronze_table(table_name: str, partition: str = None) -> Tuple[pd.DataFrame, str]:
    """Load a bronze table from Data Lake."""
    base_path = BRONZE_TABLES[table_name]

    if partition is None:
        partition = _find_latest_partition('bronze', base_path)
        if partition is None:
            raise ValueError(f"No partitions found for {table_name}")

    path = f"{base_path}/{partition}/data.json"
    print(f"  Loading {table_name} from bronze/{path}")

    wait_for_file(
        'bronze', path,
        timeout_seconds=WAIT_TIMEOUT_SECONDS,
        poll_interval=WAIT_POLL_INTERVAL,
        raise_on_timeout=True
    )

    json_data = read_from_datalake('bronze', path)
    df = pd.DataFrame(json.loads(json_data))
    print(f"  Loaded {len(df)} records from {table_name}")
    return df, partition


def _write_gold_table(df: pd.DataFrame, table_name: str, partition: str, source_tables: List[str]) -> str:
    """Write a gold table to Data Lake."""
    gold_path = build_gold_path(table_name, partition, 'data.csv')
    write_to_datalake('gold', gold_path, df.to_csv(index=False))

    write_metadata('gold', gold_path,
        source_layer='bronze',
        source_path="bronze/client/*/",
        extra={
            'table': table_name,
            'record_count': len(df),
            'source_tables': source_tables,
            'partition': partition,
        }
    )
    print(f"  Wrote {len(df)} records to gold/{gold_path}")
    return gold_path


# =============================================================================
# TASK FUNCTIONS
# =============================================================================

def create_fact_invoice_task(**context):
    """Create fact_invoice from invoice_item."""
    print("Creating fact_invoice...")
    df, partition = _load_bronze_table('invoice_item')
    result = transform_fact_invoice(df)
    path = _write_gold_table(result, 'fact_invoice', partition, ['invoice_item'])
    return {'table': 'fact_invoice', 'records': len(result), 'path': path}


def create_fact_invoice_detail_task(**context):
    """Create fact_invoice_detail from invoice_header."""
    print("Creating fact_invoice_detail...")
    df, partition = _load_bronze_table('invoice_header')
    result = transform_fact_invoice_detail(df)
    path = _write_gold_table(result, 'fact_invoice_detail', partition, ['invoice_header'])
    return {'table': 'fact_invoice_detail', 'records': len(result), 'path': path}


def create_fact_order_item_task(**context):
    """Create fact_order_item from order_item."""
    print("Creating fact_order_item...")
    df, partition = _load_bronze_table('order_item')
    result = transform_fact_order_item(df)
    path = _write_gold_table(result, 'fact_order_item', partition, ['order_item'])
    return {'table': 'fact_order_item', 'records': len(result), 'path': path}


def create_dim_customer_task(**context):
    """Create dim_customer from customer_address."""
    print("Creating dim_customer...")
    df, partition = _load_bronze_table('customer_address')
    result = transform_dim_customer(df)
    path = _write_gold_table(result, 'dim_customer', partition, ['customer_address'])
    return {'table': 'dim_customer', 'records': len(result), 'path': path}


def create_dim_lessee_task(**context):
    """Create dim_lessee by joining report_lessee with contract aggregations."""
    print("Creating dim_lessee...")
    report_lessee, partition = _load_bronze_table('report_lessee')
    report_contract, _ = _load_bronze_table('report_contract', partition)

    result = create_dim_lessee(report_lessee, report_contract)
    path = _write_gold_table(result, 'dim_lessee', partition, ['report_lessee', 'report_contract'])
    return {'table': 'dim_lessee', 'records': len(result), 'path': path}


def create_fact_general_ledger_task(**context):
    """Create fact_general_ledger from general_ledger."""
    print("Creating fact_general_ledger...")
    df, partition = _load_bronze_table('general_ledger')
    result = transform_fact_general_ledger(df)
    path = _write_gold_table(result, 'fact_general_ledger', partition, ['general_ledger'])
    return {'table': 'fact_general_ledger', 'records': len(result), 'path': path}


def create_margin_invoice_task(**context):
    """Create margin_invoice by joining all bronze tables."""
    print("Creating margin_invoice (joining all tables)...")

    invoice_item, partition = _load_bronze_table('invoice_item')
    invoice_header, _ = _load_bronze_table('invoice_header', partition)
    order_item, _ = _load_bronze_table('order_item', partition)
    customer_address, _ = _load_bronze_table('customer_address', partition)

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


def create_margin_rebate_task(**context):
    """Create margin_rebate by aggregating from general_ledger."""
    print("Creating margin_rebate...")

    df, partition = _load_bronze_table('general_ledger')
    fact_gl = transform_fact_general_ledger(df)
    result = create_margin_rebate(fact_gl)

    path = _write_gold_table(result, 'margin_rebate', partition, ['general_ledger'])
    return {'table': 'margin_rebate', 'records': len(result), 'path': path}


def update_synapse_views_task(**context):
    """Create or update Synapse views for all gold tables."""
    print("Updating Synapse views...")
    results = create_gold_views()

    created = sum(1 for v in results.values() if v == 'created')
    skipped = sum(1 for v in results.values() if v == 'skipped')
    errors = sum(1 for v in results.values() if v == 'error')

    print(f"Synapse views: {created} created, {skipped} skipped, {errors} errors")
    return results


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
    schedule=[BRONZE_CLIENT_DATA],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold', 'client', 'facts', 'dimensions', 'medallion'],
    doc_md=__doc__,
) as dag:

    # Fact tables
    fact_invoice = PythonOperator(
        task_id='create_fact_invoice',
        python_callable=create_fact_invoice_task,
        outlets=[GOLD_FACT_INVOICE],
    )

    fact_invoice_detail = PythonOperator(
        task_id='create_fact_invoice_detail',
        python_callable=create_fact_invoice_detail_task,
        outlets=[GOLD_FACT_INVOICE_DETAIL],
    )

    fact_order_item = PythonOperator(
        task_id='create_fact_order_item',
        python_callable=create_fact_order_item_task,
        outlets=[GOLD_FACT_ORDER_ITEM],
    )

    fact_general_ledger = PythonOperator(
        task_id='create_fact_general_ledger',
        python_callable=create_fact_general_ledger_task,
        outlets=[GOLD_FACT_GENERAL_LEDGER],
    )

    # Dimension tables
    dim_customer = PythonOperator(
        task_id='create_dim_customer',
        python_callable=create_dim_customer_task,
        outlets=[GOLD_DIM_CUSTOMER],
    )

    dim_lessee = PythonOperator(
        task_id='create_dim_lessee',
        python_callable=create_dim_lessee_task,
        outlets=[GOLD_DIM_LESSEE],
    )

    # Reporting tables
    margin_invoice = PythonOperator(
        task_id='create_margin_invoice',
        python_callable=create_margin_invoice_task,
        outlets=[GOLD_MARGIN_INVOICE],
    )

    margin_rebate = PythonOperator(
        task_id='create_margin_rebate',
        python_callable=create_margin_rebate_task,
        outlets=[GOLD_MARGIN_REBATE],
    )

    # Synapse view creation (optional - runs if SYNAPSE_SERVER is configured)
    synapse_views = PythonOperator(
        task_id='update_synapse_views',
        python_callable=update_synapse_views_task,
    )

    # Dependencies
    # Fact and dimension tables feed into margin_invoice
    [fact_invoice, fact_invoice_detail, fact_order_item, dim_customer] >> margin_invoice
    fact_general_ledger >> margin_rebate
    # All tables must complete before Synapse views are updated
    [margin_invoice, margin_rebate, dim_lessee] >> synapse_views
