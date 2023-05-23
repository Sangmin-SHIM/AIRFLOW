from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "sangminSHIM",
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_catchup_and_backfill_V1',
    default_args=default_args,
    start_date=datetime(2023, 5, 23),
    schedule_interval='@daily',
    catchup=True
) as dag:
    task1 = BashOperator(
        task_id="my_first_task",
        bash_command="echo 'Hello World! This is my first DAG'",
    )