"""
Bronze Layer: Sales Data Ingestion

Demonstrates: SENSOR + DATASET + TRIGGER

1. SENSOR: Waits for source system to be ready (simulated)
2. DATASET: Publishes BRONZE_SALES when complete
3. TRIGGER: Optionally triggers Silver DAG directly

Schedule: @hourly
Path: bronze/erp/sales/{YYYY-MM-DD-HH}/raw.json
"""
import json
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from lib.datalake import write_to_datalake, write_metadata, file_exists
from lib.utils import get_date_hour, build_bronze_path
from lib.datasets import BRONZE_SALES


# =============================================================================
# SENSOR: Custom sensor to check if source system is ready
# =============================================================================
class SourceSystemSensor(BaseSensorOperator):
    """
    Sensor that waits for source system to be ready.

    In production, this would check:
    - API health endpoint
    - Database connectivity
    - File drop location
    """

    def __init__(self, source_name: str, **kwargs):
        super().__init__(**kwargs)
        self.source_name = source_name

    def poke(self, context: Context) -> bool:
        """Check if source system is ready."""
        self.log.info(f"Checking if {self.source_name} is ready...")

        # Simulated check - in production, call actual health endpoint
        # Example: response = requests.get(f"{API_URL}/health")
        # return response.status_code == 200

        # For demo, always return True
        self.log.info(f"{self.source_name} is ready!")
        return True


# =============================================================================
# TASKS
# =============================================================================
def ingest_sales(**context):
    """Ingest sales data from source and write to Bronze layer."""
    partition = get_date_hour(context)
    execution_date = context['ds']

    # Simulated raw data from ERP system
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
    })

    print(f"Bronze ingestion complete: {len(df)} records → {path}")
    return path


def decide_trigger_method(**context):
    """
    Branch: Decide whether to use Dataset trigger or direct Trigger.

    This demonstrates how you can choose between:
    - Dataset-based trigger (automatic, event-driven)
    - TriggerDagRunOperator (explicit, immediate)
    """
    # For demo, always use dataset trigger
    # In production, could check conditions like:
    # - Is this a backfill? Use direct trigger
    # - Is this real-time? Use dataset trigger
    use_direct_trigger = False

    if use_direct_trigger:
        return 'trigger_silver_directly'
    else:
        return 'dataset_trigger_complete'


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
    'bronze_sales_ingestion',
    default_args=default_args,
    description='Bronze: Ingest raw sales data (Sensor + Dataset + Trigger demo)',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'sales', 'medallion', 'sensor', 'dataset', 'trigger'],
) as dag:

    # -------------------------------------------------------------------------
    # SENSOR: Wait for source system to be ready
    # -------------------------------------------------------------------------
    wait_for_source = SourceSystemSensor(
        task_id='wait_for_source_system',
        source_name='ERP System',
        poke_interval=30,        # Check every 30 seconds
        timeout=60 * 5,          # Timeout after 5 minutes
        mode='poke',             # 'poke' = hold worker, 'reschedule' = release
    )

    # -------------------------------------------------------------------------
    # TASK: Ingest data (publishes DATASET)
    # -------------------------------------------------------------------------
    ingest = PythonOperator(
        task_id='ingest_sales',
        python_callable=ingest_sales,
        outlets=[BRONZE_SALES],  # DATASET: Publishes this dataset
    )

    # -------------------------------------------------------------------------
    # BRANCH: Choose trigger method
    # -------------------------------------------------------------------------
    branch = BranchPythonOperator(
        task_id='decide_trigger_method',
        python_callable=decide_trigger_method,
    )

    # -------------------------------------------------------------------------
    # OPTION A: Dataset trigger (automatic - Silver DAG listens)
    # -------------------------------------------------------------------------
    dataset_complete = EmptyOperator(
        task_id='dataset_trigger_complete',
    )

    # -------------------------------------------------------------------------
    # OPTION B: Direct TRIGGER (explicit - call Silver DAG)
    # -------------------------------------------------------------------------
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_directly',
        trigger_dag_id='silver_sales_transform',
        wait_for_completion=False,
        conf={'triggered_by': 'bronze_sales_ingestion'},
    )

    # -------------------------------------------------------------------------
    # FLOW
    # -------------------------------------------------------------------------
    # Sensor → Ingest → Branch → (Dataset OR Trigger)
    wait_for_source >> ingest >> branch
    branch >> [dataset_complete, trigger_silver]
