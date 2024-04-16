from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.operators.empty import EmptyOperator


from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.models.param import Param

import pandas as pd
import random
import folium
from branca.element import Template, MacroElement
import os
import gzip
import shutil
from zipfile import ZipFile 

import aiohttp
import aiofiles
import asyncio
from simpledbf import Dbf5
import time
from coordinate import clean_df_coordinate
from csv_process import (
    generate_template,
    convert_txt_to_csv_2017_2022, 
    clean_pd_df_election_2017_2022, 
    change_col_names_2017_2022,
    prepare_for_fusion_with_coordinate_2017_2022,
    match_col_coordinate_with_election,
    process_fusioned_data_2017_2022, 
    convert_txt_to_csv_2012,
    get_headers_2012,
    clean_pd_df_election_2012,
    get_pivot_table_2012)
    
from database import get_db_connection
INPUT_FOLDER=Variable.get("DATA_INPUT_FOLDER")
CSV_FOLDER=Variable.get("CSV_FOLDER")

def download_social_data_sync():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_social_data())

async def download_social_data():
    task_identifiers=["URL_CSV_CRIME","URL_CSV_DPAE","URL_DEATH_17","URL_BIRTH_17"]
    zip_files=["URL_DEATH_17","URL_BIRTH_17"]
    
    for identifier in task_identifiers:
        if identifier in zip_files:
            ext = "zip"
        else:
            ext = "csv"
            
        url = Variable.get(f"{identifier}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    print("download success")
                    f = await aiofiles.open(f'{INPUT_FOLDER}/social_data/{identifier}.{ext}', mode="wb")
                    await f.write(await response.read())
                    await f.close()
                    
                else:
                    print("failed")    

def download_geo_data_sync():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_geo_data())
    
async def download_geo_data():
    task_identifier = "GEO_BUREAUX_DE_VOTE"
    url = Variable.get(f"URL_{task_identifier}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                print("download success")
                
                f = await aiofiles.open('./geo_bureaux_de_vote.gz', mode="wb")
                
                await f.write(await response.read())
                await f.close()
                
                with gzip.open('./geo_bureaux_de_vote.gz', 'rb') as f_in:
                    with open(f'{INPUT_FOLDER}/geo_bureaux_de_vote.csv', 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                os.remove('./geo_bureaux_de_vote.gz')
            else:
                print("failed")
    
def download_election_data_sync():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_election_data())
    
async def download_election_data():
    task_identifiers=["PR_12","PR_17_T1","PR_17_T2","PR_22_T1","PR_22_T2"]

    for identifier in task_identifiers:
        url = Variable.get(f"URL_TXT_{identifier}")
        original_file = Variable.get(f"ORIGINAL_TXT_{identifier}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    print("download success")
                    f = await aiofiles.open(f'{INPUT_FOLDER}/{original_file}', mode="wb")
                    await f.write(await response.read())
                    await f.close()
                    
                else:
                    print("failed")
            
def generate_coordinate_data():
    csv_file = Variable.get("ORIGINAL_CSV_GEO_BUREAUX_DE_VOTE")
    df_coordinate = pd.read_csv(f'{INPUT_FOLDER}/{csv_file}')
    
    df_coordinate= clean_df_coordinate(df_coordinate)
    df_coordinate=df_coordinate.sort_values(['commune_code','code'],ascending=True)

    df_coordinate.to_csv(f'{INPUT_FOLDER}/coordinate/coordinate.csv')

def process_social_data_sync():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_social_data())

async def process_social_data():
    zip_files=[
        {"identifier":"URL_BIRTH_17","file_name":"nais2017.dbf"},{"identifier":"URL_DEATH_17","file_name":"dec2017.dbf"}]

    for zip_file in zip_files:
        with ZipFile(f'{INPUT_FOLDER}/social_data/{zip_file["identifier"]}.zip', 'r') as zObject: 
            zObject.extract(f'{zip_file["file_name"]}', path=f'{INPUT_FOLDER}/social_data') 
        zObject.close() 
    
    time.sleep(5)
    for zip_file in zip_files:
        dbf = Dbf5(f'{INPUT_FOLDER}/social_data/{zip_file["file_name"]}')
        dbf.to_csv(f'{INPUT_FOLDER}/social_data/{zip_file["file_name"]}.csv')
        
        os.remove(f'{INPUT_FOLDER}/social_data/{zip_file["identifier"]}.zip')
        os.remove(f'{INPUT_FOLDER}/social_data/{zip_file["file_name"]}')

def process_2017_2022_result():
    task_identifiers=["PR_17_T1","PR_17_T2","PR_22_T1","PR_22_T2"]
    read_coordinate=pd.read_csv(f'{INPUT_FOLDER}/coordinate/coordinate.csv')
    df_coordinate=match_col_coordinate_with_election(read_coordinate)
    
    for identifier in task_identifiers:
        # ----------------------
        # Variables : txt_file, csv_file
        # ----------------------
        txt_file=Variable.get(f'ORIGINAL_TXT_{identifier}')
        csv_file=Variable.get(f'CSV_{identifier}')
        
        # ----------------------
        # Election data cleaning
        # ----------------------
        headers=convert_txt_to_csv_2017_2022(f'{INPUT_FOLDER}/{txt_file}', f'{CSV_FOLDER}/{csv_file}')
        df_election=pd.read_csv(f'{CSV_FOLDER}/{csv_file}', delimiter=';', encoding='latin-1', header=None,names=headers, engine='python')
        df_election=clean_pd_df_election_2017_2022(df_election)
        df_election=change_col_names_2017_2022(df_election)
        df_election=clean_pd_df_election_2017_2022(df_election)
        df_election=prepare_for_fusion_with_coordinate_2017_2022(df_election)
    
        # ----------------------
        # Fusion : Election data with coordinate data
        # ----------------------
        df_INNER_JOIN = pd.merge(df_election,df_coordinate, on=['Code du b.vote','Code du département'])
        df_INNER_JOIN=process_fusioned_data_2017_2022(df_INNER_JOIN)
    
        # ----------------------
        # Color for each candidate
        # ----------------------
        list_nom=[]
        list_prenom=[]

        list_nom.append(df_INNER_JOIN['NOM_candidat_le_plus_voté'].tolist())
        list_prenom.append(df_INNER_JOIN['PRENOM_candidat_le_plus_voté'].tolist())

        # Contained 2nd Array Dimension in List -> Remove 2nd Array, and take the elements in list 
        list_nom = sum(list_nom, [])
        list_prenom = sum(list_prenom, [])

        # Remove Duplicated elements
        dict_nom = dict.fromkeys(list_nom)
            
        # Color for Each Politic Party
        for key, value in dict_nom.items():
            dict_nom[key]="#%06x" % random.randint(0, 0xFFFFFF)

        dict_nom['MACRON']="#FFB400"
        dict_nom['LE PEN']="#2A25C7"
        
        # ----------------------
        # Map
        # ----------------------
        map = folium.Map(location=[46.91160617052484, 2.153649265809747]
                       , zoom_start=5.5)

        df_INNER_JOIN.apply(lambda row: folium.Circle(location=[row.loc['latitude'], row.loc['longitude']],
                                             radius=100,
                                             popup=row.loc['NOM_candidat_le_plus_voté'],
                                             fill=True, 
                                             tooltip="""
                                                     Nom : <b>"""+ str(row['NOM_candidat_le_plus_voté']) +"""</b><br>
                                                     Prénom : <b>"""+ str(row['PRENOM_candidat_le_plus_voté']) +"""</b><br>
                                                     % Voix/Exp : <b>"""+ str(row['% Voix/Exp_le_plus_voté']) +"""</b><br>
                                                     
                                                    
                                                     
                                                     """, 
                                             color=dict_nom[row.loc['NOM_candidat_le_plus_voté']], 
                                             fill_color=dict_nom[row['NOM_candidat_le_plus_voté']],
                                             opacity=0.8,
                                             fill_opacity=0.7)
                                             .add_to(map), axis=1)    
        
        # ----------------------
        # Legend, Template
        # ----------------------
        legend_label_list = [{'text': key, 'color': value} for key, value in dict_nom.items()]
        template = generate_template(identifier,legend_label_list)
        macro = MacroElement()
        macro._template = Template(template)
        
        # ----------------------
        # Save HTML
        # ----------------------
        map.get_root().add_child(macro)
        map.save(f'html_output/{identifier}.html')
    
def process_2012_result():
    identifier="PR_12"
    tour_list=[1,2]
    read_coordinate=pd.read_csv(f'{INPUT_FOLDER}/coordinate/coordinate.csv')
    df_coordinate=match_col_coordinate_with_election(read_coordinate)

    # ----------------------
    # Variables : txt_file, csv_file
    # ----------------------
    
    txt_file=Variable.get(f'ORIGINAL_TXT_{identifier}')
    csv_file=Variable.get(f'CSV_{identifier}')

    # ----------------------
    # Election data cleaning
    # ----------------------
    convert_txt_to_csv_2012(f'{INPUT_FOLDER}/{txt_file}', f'{CSV_FOLDER}/{csv_file}')
    df_election=pd.read_csv(f'{CSV_FOLDER}/{csv_file}', delimiter=';',encoding='latin-1', engine='python', names=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14])
    df_election=get_headers_2012(df_election)
    
    df_election=clean_pd_df_election_2012(df_election)
    df_election=clean_pd_df_election_2012(df_election)
    
    df_PIVOT=get_pivot_table_2012(df_election)
    
    # ----------------------
    # Fusion : Election data with coordinate data
    # ----------------------
    df_INNER_JOIN = pd.merge(df_PIVOT,df_coordinate, on=['Code du b.vote','Code du département'])
    
    # TOUR 1, TOUR 2
    for tour in tour_list:
    # ----------------------
    # Color for each candidate
    # ----------------------
        map = folium.Map(location=[46.91160617052484, 2.153649265809747]
                    , zoom_start=5.5)

        tour_string=''
        if tour ==1:
            tour_string='1er'
        else:
            tour_string='2ème'
        
        # ---------------
        # Color
        # ---------------
        list_candidate=[]

        list_candidate.append(df_INNER_JOIN[df_INNER_JOIN['Tour'] == tour]['Candidat le plus voté'].tolist())

        # Contained 2nd Array Dimension in List -> Remove 2nd Array, and take the elements in list 
        list_candidate = sum(list_candidate, [])

        # Remove Duplicated elements
        dict_nom = dict.fromkeys(list_candidate)
        list_candidat = list(set(zip(list_candidate)))

        # Color for Each Politic Party
        for key, value in dict_nom.items():
            dict_nom[key]="#%06x" % random.randint(0, 0xFFFFFF)
                
        # ---------------
        # Legend, Template
        # ---------------
        legend_label_list = [{'text': key, 'color': value} for key, value in dict_nom.items()]
        
        title = f'{identifier} {tour_string} tour'
        
        template = generate_template(title,legend_label_list)
        macro = MacroElement()  
        macro._template = Template(template)
        
        # ---------------
        # Map
        # ---------------
        df_INNER_JOIN[df_INNER_JOIN['Tour'] == tour].apply(lambda row: folium.Circle(location=[row.loc['latitude'], row.loc['longitude']],
                                                    radius=100,
                                                    popup=row.loc['Candidat le plus voté'],
                                                    fill=True, 
                                                    tooltip="""
                                                            Candidat : <b>"""+ str(row['Candidat le plus voté']) +"""</b><br>
                                                            % Voix/Exp : <b>"""+ str(row['% Voix/Exp_le_plus_voté']) +"""</b><br>

                                                            """, 
                                                    color=dict_nom[row.loc['Candidat le plus voté']], 
                                                    fill_color=dict_nom[row['Candidat le plus voté']],
                                                    opacity=0.8,
                                                    fill_opacity=0.7)
                                                    .add_to(map), axis=1)  
        # ---------------
        # Save HTML
        # ---------------
        map.get_root().add_child(macro)
        map.save(f'html_output/{identifier}_T{tour}.html')            

def upsert_social_data_to_db():
    task_csv_to_db_idenfiers=[{"file_name":"dec2017.dbf.csv", "table_name":"death", "delimiter":","},{"file_name":"nais2017.dbf.csv", "table_name":"birth", "delimiter":","},{"file_name":"URL_CSV_CRIME.csv", "table_name":"crime", "delimiter":";"},{"file_name":"URL_CSV_DPAE.csv", "table_name":"dpae", "delimiter":";"}]
    
    for task_csv_to_db_idenfier in task_csv_to_db_idenfiers:
        file_name=task_csv_to_db_idenfier["file_name"]
        table_name=task_csv_to_db_idenfier["table_name"]
        delimiter=task_csv_to_db_idenfier["delimiter"]
        
        df = pd.read_csv(f'{INPUT_FOLDER}/social_data/{file_name}', delimiter=delimiter)
        
        conn = get_db_connection()
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    
            
dag = DAG(
    'france_president_election_with_social_impact',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False,
)

download_geo_data_task = PythonOperator(
    task_id='download_geo_data',
    python_callable=download_geo_data_sync,
    dag=dag
)

download_election_data_task = PythonOperator(
    task_id='download_election_data',
    python_callable=download_election_data_sync,
    dag=dag
)

download_social_data_task = PythonOperator(
    task_id='download_social_data',
    python_callable=download_social_data_sync,
    dag=dag
)

generate_coordinate_data_task = PythonOperator(
    task_id='generate_coordinate_data',
    python_callable=generate_coordinate_data,
    dag=dag
)

process_2017_2022_result_task = PythonOperator(
    task_id='process_2017_2022_result',
    python_callable=process_2017_2022_result,
    dag=dag
)

process_2012_result_task = PythonOperator(
    task_id='process_2012_result',
    python_callable=process_2012_result,
    dag=dag
)

process_social_data_task = PythonOperator(
    task_id='process_social_data',
    python_callable=process_social_data_sync,
    dag=dag
)

upsert_social_data_to_db_task = PythonOperator(
    task_id='upsert_social_data_to_db',
    python_callable=upsert_social_data_to_db,
    dag=dag
)

main_task = EmptyOperator(
    task_id='main',
)

# ----------------------
# TASK -----------------
# ----------------------

# ----------------------
# (1) Election Data ----
# ----------------------
main_task>>[download_geo_data_task,download_election_data_task] >> generate_coordinate_data_task >> [process_2017_2022_result_task, process_2012_result_task]

# ----------------------
# (2) Social Data ------
# ----------------------
main_task>>download_social_data_task >> process_social_data_task >> upsert_social_data_to_db_task
