from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "sangminSHIM",
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
    }

with DAG(
    dag_id="first_dag_v3",
    default_args=default_args,
    description="This is our first DAG",
    start_date=datetime(2023, 5, 23),
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="my_first_task",
        bash_command="echo 'Hello World! This is my first DAG'",
    )

    task2 = BashOperator(
        task_id="my_second_task",
        bash_command="echo 'It will be running after task1. This is my second DAG'",
    )

    task3 = BashOperator(
        task_id="my_third_task",
        bash_command="echo 'It will be running after task1 at the same time as task2. This is my third DAG'",
    )

    task1.set_downstream(task2)
    task1.set_downstream(task3)