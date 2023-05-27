from requests_custom import RequestCustom
from csv_custom import CsvCustom

from default_args import get_default_args
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
import re
import pandas as pd
import os
import sqlite3
import csv 

# Folder path containing the CSV files
current_path = os.getcwd()
parent_directory = os.path.dirname(current_path)


# Site : Quel Frigo
def execute_data_quel_frigo():
    number=1
    site_quel_frigo = RequestCustom(url=f"https://www.quelfrigo.com/all-products/page/{number}.html", site_name="quelfrigo")
    status = site_quel_frigo.get_status()

    csv_quelfrigo=CsvCustom("quelfrigo")
    csv_quelfrigo.create_csv()

    while status == 200:
        #listes_prods
        soup = site_quel_frigo.get_soup()
        listes_prods=soup.find_all("div", attrs={"class":"listes_prods"})
        for prods in listes_prods:
            title = prods.find("span", attrs={"itemprop":"model"}).get_text()
            link_element = prods.find("a", attrs={"itemprop": "url"})
            link = link_element['href'] if link_element else ""
            description = prods.find("div", attrs={"class":"marqueDescription"}).find("p").get_text()
            price = prods.find("span", attrs={"class":"unite2"}).get_text()

            description = description.strip()
            price = ''.join(price.split())
            
            file=csv_quelfrigo.write_csv(title=title, link=link, description=description, price=price)
        number=number+1
        site_quel_frigo = RequestCustom(url=f"https://www.quelfrigo.com/all-products/page/{number}.html", site_name="quelfrigo")
        status = site_quel_frigo.get_status()
        
    csv_quelfrigo.close_csv(file)  
    return "execute_data_quel_frigo DONE !"

# Site : Energy Star
def execute_data_energy_star():
    number=1

    site_energy_star = RequestCustom(url=f"https://www.energystar.gov/productfinder/product/certified-residential-refrigerators/?formId=077786-589-4-8882-821429358&scrollTo=3719&search_text=&low_price=&high_price=&price_filter=Under+%24500&price_filter=%24500-%24749&price_filter=%24750-%24999&price_filter=%241000-%241499&price_filter=%241500-%241999&price_filter=%242000-%242999&price_filter=%243000+and+up&price_filter=Price+not+available&is_most_efficient_filter=0&height_in_isopen=0&width_in_isopen=0&brand_name_isopen=0&markets_filter=United+States&markets_filter=Canada&zip_code_filter=&product_types=Select+a+Product+Category&sort_by=percent_less_energy_use_than_us_federal_standard&sort_direction=desc&currentZipCode=93300&page_number={number}", site_name="energy_star")
    status = site_energy_star.get_status()

    csv_energystar=CsvCustom("energystar")
    csv_energystar.create_csv()

    while status == 200:
        #listes_prods
        soup = site_energy_star.get_soup()

        listes_prods=soup.find_all("div", attrs={"class":"row certified-residential-refrigerators"})

        for prods in listes_prods:
            title = prods.find("div", attrs={"class":"title"}).get_text()
            link ="https://www.energystar.gov"+prods.find("div",attrs={"class":"title"}).find("a")['href']
            description = prods.find("div", attrs={"class":"value"}).get_text()
            price_element = prods.find("a", attrs={"class": "priceRange"})
            price = price_element.get_text() if price_element else ""

            price = ''.join(price.split())
            title = title.strip()
            description = description.strip()
            
            file=csv_energystar.write_csv(title=title, link=link, description=description, price=price)
        number=number+1
        site_energy_star = RequestCustom(url=f"https://www.energystar.gov/productfinder/product/certified-residential-refrigerators/?formId=077786-589-4-8882-821429358&scrollTo=3719&search_text=&low_price=&high_price=&price_filter=Under+%24500&price_filter=%24500-%24749&price_filter=%24750-%24999&price_filter=%241000-%241499&price_filter=%241500-%241999&price_filter=%242000-%242999&price_filter=%243000+and+up&price_filter=Price+not+available&is_most_efficient_filter=0&height_in_isopen=0&width_in_isopen=0&brand_name_isopen=0&markets_filter=United+States&markets_filter=Canada&zip_code_filter=&product_types=Select+a+Product+Category&sort_by=percent_less_energy_use_than_us_federal_standard&sort_direction=desc&currentZipCode=93300&page_number={number}", site_name="energy_star")
        status = site_energy_star.get_status()
        
    csv_energystar.close_csv(file) 
    return "execute_data_energy_star DONE !" 

# Site : Refrigerator Pro
def execute_data_refrigerator_pro():
    company_dict={}
    listes_uri=[
    'Side-by-Side-Refrigerators.html',
    'bottom-freezer-refrigerator.html',
    'Side-by-Side-Refrigerators.html',
    'Top-Freezer-Refrigerators.html',
    'counter-depth-refrigerator.html',    
    ]
    site_refrigerator_pro = RequestCustom(url=f"https://www.refrigeratorpro.com/{listes_uri[0]}", site_name="refrigerator_pro")

    soup = site_refrigerator_pro.get_soup()

    listes_prods_title=soup.find("ol").find_all("h3")
    listes_prods_link=soup.find("ol").find_all("a")
    listes_prods_description=soup.find("ol").find_all("p")

    for title in listes_prods_title:
        title = title.get_text().strip()
        for link in listes_prods_link:
                    if title == "General Electric":
                        title = "GE_"
                    if title.upper() in link["href"].strip().replace("-"," ").upper():                    
                        if title not in company_dict.keys():
                            company_dict[title]=link["href"]
                        continue
    for description in listes_prods_description[0].find_all("h3"):
        if description.find_next_sibling("p"):
            text = description.find_next_sibling("p").get_text()
            break

    # Remove phrases starting with "Click for ..."
    clean_text = re.sub(r"Click here for .*?Reviews", "", text, flags=re.DOTALL)
    clean_text = re.sub(r"Click for .*?Reviews", "", clean_text, flags=re.DOTALL)
    clean_text = re.sub(r"Check out .", "", clean_text, flags=re.DOTALL)
    # Remove words with line breaks that don't have a period (".") at the end
    clean_text = re.sub(r"(?m)^\w+(?: \w+)*\n(?!\w*\.)", "", clean_text)
    clean_text = clean_text.replace('\n',"").split('. ')

    csv_refrigerator_pro=CsvCustom("refrigerator_pro")
    csv_refrigerator_pro.create_csv()
    for index, (title, link) in enumerate(company_dict.items(), start=0):
        file=csv_refrigerator_pro.write_csv(title=title, link=link, description=clean_text[index], price="")
        
    csv_refrigerator_pro.close_csv(file)  
    return "execute_data_refrigerator_pro DONE !"

def execute_data_combined():
    # Folder path containing the CSV files
    current_path = os.getcwd()
    parent_directory = os.path.dirname(current_path)

    folder_path = f"{parent_directory}/AIRFLOW/csv/"

    # Initialize an empty DataFrame
    combined_data = pd.DataFrame()

    # Iterate over the files in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            print(file_name)

            file_path = os.path.join(folder_path, file_name)
            # Read each CSV file
            data = pd.read_csv(file_path)
            # Append the data to the combined_data DataFrame
            combined_data = pd.concat([combined_data, data])

    # Write the combined data to a new CSV file
    combined_data.to_csv("csv/combined.csv", index=False)
    return "execute_data_combined DONE !"

def save_database():
    # Connecting to the geeks database
    connection = sqlite3.connect('airflow.db')
    
    # Creating a cursor object to execute
    # SQL queries on a database table
    cursor = connection.cursor()
    
    # Table Definition
    create_table = '''CREATE TABLE fridge(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    link TEXT,
                    description TEXT,
                    price TEXT
                    );
                    '''
    
    # Creating the table into our
    # database
    cursor.execute(create_table)
    
    # Opening the person-records.csv file
    file = open('csv/combined.csv')
    
    # Reading the contents of the
    # person-records.csv file
    contents = csv.reader(file)
    # Skip the first row
    next(contents)
    # SQL query to insert data into the
    # person table
    insert_records = "INSERT INTO fridge (title, link, description, price) VALUES(?, ?, ?, ?)"
    
    # Importing the contents of the file
    # into our person table
    cursor.executemany(insert_records, contents)
    
    # Committing the changes
    connection.commit()
    
    # closing the database connection
    connection.close()
    return "save_database DONE !"
def find_key_words_of_description_by_frequency():
    df = pd.read_csv('csv/combined.csv')
    # Count the occurrences of each word in a specific column
    word_counts_description = df['description'].value_counts()

    # Convert the Series to a DataFrame
    word_counts_df = pd.DataFrame({'word': word_counts_description.index, 'count': word_counts_description.values})
    word_counts_df.to_csv("csv/word_counts_description.csv", index=False)

def find_key_words_of_title_by_frequency():
    df = pd.read_csv('csv/combined.csv')
    # Count the occurrences of each word in a specific column
    word_counts_title = df['title'].value_counts()

    # Convert the Series to a DataFrame
    word_counts_df = pd.DataFrame({'word': word_counts_title.index, 'count': word_counts_title.values})
    word_counts_df.to_csv("csv/word_counts_title.csv", index=False)

with DAG(
    default_args=get_default_args(),
    dag_id="Data_Scrapping",
    description="3 Sites - Quel Frigo, Energy Star, Refrigerator Pro",
    start_date=datetime(2023, 5, 26),
    schedule_interval="@weekly",
) as dag:
    task1 = PythonOperator(
        task_id="site_quel_frigo", 
        python_callable=execute_data_quel_frigo,
        )
    task2 = PythonOperator(
        task_id="site_energy_star", 
        python_callable=execute_data_energy_star,
        )
    task3 = PythonOperator(
        task_id="site_refrigerator_pro",
        python_callable=execute_data_refrigerator_pro,
        )
    task4 = PythonOperator(
        task_id="data_combined",
        python_callable=execute_data_combined,
        )
    task5 = PythonOperator(
        task_id="save_database",
        python_callable=save_database,
        )
    task6 = PythonOperator(
        task_id="find_key_words_of_description_by_frequency",
        python_callable=find_key_words_of_description_by_frequency,
        )
    task7 = PythonOperator(
        task_id="find_key_words_of_title_by_frequency",
        python_callable=find_key_words_of_title_by_frequency,
        )

    [task1,task2,task3] >> task4 >> [task5,task6,task7]
    