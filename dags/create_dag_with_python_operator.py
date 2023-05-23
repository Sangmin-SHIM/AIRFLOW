from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    "owner": "sangminSHIM",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids="python_get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="python_get_name", key="last_name")
    age = ti.xcom_pull(task_ids="python_get_age", key="age")
    print(f"{first_name} & {last_name} : my age is {age}. (DAG by Python Operator)")

def get_name(ti):
    ti.xcom_push(key="first_name", value="Sangmin")
    ti.xcom_push(key="last_name", value="Shim")

def get_age(ti):
    ti.xcom_push(key="age", value=30)

with DAG(
    default_args=default_args,
    dag_id="our_dag_with_python_operator_V5",
    description="This is our DAG with Python Operator",
    start_date=datetime(2023, 5, 23),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="python_greet", 
        python_callable=greet,
        # op_kwargs={"age": 30}
        )
    
    task2 = PythonOperator(
        task_id="python_get_name",
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id="python_get_age",
        python_callable=get_age
    )

    [task2, task3] >> task1
