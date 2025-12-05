"""
Example DAG to verify Airflow installation.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_hello():
    """Simple function to print hello."""
    print("Hello from Airflow!")
    return "Hello task completed"


def print_date(**context):
    """Print the execution date."""
    print(f"Execution date: {context['ds']}")
    return context['ds']


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Print hello using Python
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    # Task 2: Print date using Python
    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    # Task 3: Run a bash command
    bash_task = BashOperator(
        task_id='bash_command',
        bash_command='echo "Current time: $(date)"',
    )

    # Task 4: Final task
    end_task = BashOperator(
        task_id='end',
        bash_command='echo "DAG completed successfully!"',
    )

    # Define task dependencies
    hello_task >> date_task >> bash_task >> end_task
