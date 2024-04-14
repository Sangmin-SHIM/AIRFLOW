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
    
folder=Variable.get("DATA_INPUT_FOLDER")
csv_folder=Variable.get("CSV_FOLDER")

def generate_coordinate_data():
    excel=Variable.get("ORIGINAL_EXCEL_GEO_BUREAUX_DE_VOTE")
    df_coordinate = pd.read_excel(f'{folder}/{excel}')
    
    df_coordinate= clean_df_coordinate(df_coordinate)
    df_coordinate=df_coordinate.sort_values(['commune_code','code'],ascending=True)

    excel_coordinate='coordinate.xlsx'
    df_coordinate.to_excel(f'{folder}/coordinate/{excel_coordinate}')
    
def process_2017_2022_result():
    task_identifiers=["PR_17_T1","PR_17_T2","PR_22_T1","PR_22_T2"]
    read_coordinate=pd.read_excel(f'{folder}/coordinate/coordinate.xlsx')
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
        headers=convert_txt_to_csv_2017_2022(f'{folder}/{txt_file}', f'{csv_folder}/{csv_file}')
        df_election=pd.read_csv(f'{csv_folder}/{csv_file}', delimiter=';', encoding='latin-1', header=None,names=headers, engine='python')
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
    read_coordinate=pd.read_excel(f'{folder}/coordinate/coordinate.xlsx')
    df_coordinate=match_col_coordinate_with_election(read_coordinate)

    # ----------------------
    # Variables : txt_file, csv_file
    # ----------------------
    
    txt_file=Variable.get(f'ORIGINAL_TXT_{identifier}')
    csv_file=Variable.get(f'CSV_{identifier}')

    # ----------------------
    # Election data cleaning
    # ----------------------
    convert_txt_to_csv_2012(f'{folder}/{txt_file}', f'{csv_folder}/{csv_file}')
    df_election=pd.read_csv(f'{csv_folder}/{csv_file}', delimiter=';',encoding='latin-1', engine='python', names=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14])
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
            
dag = DAG(
    'france_president_election',
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

process_2017_2022_result = PythonOperator(
    task_id='process_2017_2022_result',
    python_callable=process_2017_2022_result,
    dag=dag
)

process_2012_result = PythonOperator(
    task_id='process_2012_result',
    python_callable=process_2012_result,
    dag=dag
)

generate_coordinate_data_task>>[process_2017_2022_result, process_2012_result]