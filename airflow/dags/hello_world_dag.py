"""
Hello World DAG - Simple test DAG for CI/CD validation
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'trinity',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hello_world',
    default_args=default_args,
    description='Simple test DAG for CI/CD validation',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'hello-world'],
) as dag:

    def print_hello():
        print("Hello from Trinity Airflow!")
        print(f"Current time: {datetime.now()}")
        return "Hello World completed successfully!"

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )

    check_env = BashOperator(
        task_id='check_environment',
        bash_command='echo "Airflow is running on $(hostname)" && python --version',
    )

    hello_task >> check_env
