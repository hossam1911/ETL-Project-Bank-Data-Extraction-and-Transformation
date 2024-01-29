import pandas as pd 
from bs4 import BeautifulSoup as bs 
import requests
from datetime import datetime
import sqlite3
# Importing the required libraries

def extract(url ):
    df= pd.read_html(url)
    df =df[3]
    return df

def transform(data):
    M_to_B = lambda x: round(x/1000,2)
    A=['Country/Territory',
    'UN region','IMF_Estimate','IMF_Year','WB_Estimate','WB_Year','Un_Estimate','Un_Year']
    data.columns=(A) 
    data = data[data['IMF_Estimate'] != '—']
    data = data[data['WB_Estimate'] != '—']
    data = data[data['Un_Estimate'] != '—']
    data['IMF_Estimate'] = data['IMF_Estimate'].astype(float)
    data['WB_Estimate'] = data['WB_Estimate'].astype(float)
    data['Un_Estimate'] = data['Un_Estimate'].astype(float)
    for colum in data.columns[data.dtypes == float].tolist():
        data[colum]=data[colum].astype(float).apply(M_to_B)
    return data

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
log_progress('Preliminaries complete. Initiating ETL process')

df = extract('https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29')

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, 'D:\IBM Data engineering course\Projects\Web_scraping\data.csv')

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('tefa.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, 'List_of_countries_by_GDP')

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from List_of_countries_by_GDP WHERE IMF_Estimate >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
