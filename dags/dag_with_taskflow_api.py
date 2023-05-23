from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'sangminSHIM',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

@dag(dag_id="dag_with_taskflow_api_V2",
     default_args=default_args,
     start_date=datetime(2023, 5, 23),
     schedule_interval='@daily',
     )
def hello_world_etl():
    

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name':'Sangmin',
            'last_name':'Shim'
        }
    
    @task(multiple_outputs=False)
    def get_age():
        return 30
    
    @task()
    def greet(first_name,last_name,age):
        print(f"Hello, {first_name} {last_name} ({age} years old)")

    
    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'],last_name=name_dict['last_name'],age=age)

greet_dag=hello_world_etl()
