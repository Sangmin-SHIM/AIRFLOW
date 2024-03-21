from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.models.param import Param

import pandas as pd
import re
import random
import itertools
import folium
from folium import plugins
from branca.element import Template, MacroElement
from coordinate import clean_df_coordinate
from csv_process import convert_txt_to_csv_2017_2022
    
folder=Variable.get("DATA_INPUT_FOLDER")

def generate_coordinate_data():
    excel=Variable.get("ORIGINAL_EXCEL_GEO_BUREAUX_DE_VOTE")
    df_coordinate = pd.read_excel(f'{folder}/{excel}')
    
    df_coordinate= clean_df_coordinate(df_coordinate)
    df_coordinate=df_coordinate.sort_values(['commune_code','code'],ascending=True)

    excel_coordinate='coordinate.xlsx'
    df_coordinate.to_excel(f'{folder}/coordinate/{excel_coordinate}')
    
def generate_2017_1st_result():
    txt_file=Variable.get("ORIGINAL_TXT_PR_17_T1")
    csv_file=Variable.get("CSV_PR_17_T1")
    csv_folder=Variable.get("CSV_FOLDER")
    
    convert_txt_to_csv_2017_2022(f'{folder}/{txt_file}', f'{csv_folder}/{csv_file}')
    
dag = DAG(
    'presidential_election',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False,
    params={
        "x": Param(5, type="integer", minimum=3),
        "my_int_param": 6
    }
)

generate_coordinate_data_task = PythonOperator(
    task_id='generate_coordinate_data',
    python_callable=generate_coordinate_data,
    dag=dag
)

generate_2017_1st_result_task = PythonOperator(
    task_id='generate_2017_1st_result',
    python_callable=generate_2017_1st_result,
    dag=dag
)

# generate_coordinate_data_task >> generate_2017_1st_result_task