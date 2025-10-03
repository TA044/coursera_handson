import pandas as pd 
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import numpy as np

# python3.11 -m pip install pandas
# python3.11 -m pip install bs4
# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = 'table.wikitable.sortable'
csv_path = '/home/project/exchange_rate.csv'
output_path = './Largest_banks_data.csv'
sql_connection = 'Banks.db'
table_name = 'Largest_banks'
# query_statement = 'SELECT * FROM Largest_banks'
# query_statement = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query_statement = 'SELECT Name from Largest_banks LIMIT 5'
log_file = "code_log.txt" 

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ':' + message + '\n') 
log_progress("Preliminaries complete. Initiating ETL process")

def extract(url, table_attribs):
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    all_rows = soup.select(table_attribs)
    df_list = []
    rows = all_rows[0].find_all('tr')
    for i in rows:
        cells = i.find_all(['td', 'th'])
        if len(cells) >=3:
            name = cells[1].get_text(strip=True)
            mc = cells[2].get_text(strip=True)
            df_list.append({
                'Name': name,
                'MC_USD_Billion': mc
            })
    df = pd.DataFrame(df_list)
    df = df.iloc[1:]
    df = df.reset_index(drop=True)
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    return df
df = extract(url, table_attribs)
print("Data extraction completed successfully.")
log_progress("Data extraction complete. Initiating Transformation process")

def transform(df, csv_path):
    currency = pd.read_csv(csv_path)
    exchange_rate = dict(zip(currency['Currency'], currency['Rate']))
   
    df['MC_EUR_Billion'] = [np.round(
        x*exchange_rate['EUR'],2
        ) 
        for x in df['MC_USD_Billion']]

    df['MC_GBP_Billion'] = [np.round(
        x*exchange_rate['GBP'],2
        ) 
        for x in df['MC_USD_Billion']]

    df['MC_INR_Billion'] = [np.round(
        x*exchange_rate['INR'],2
        ) 
        for x in df['MC_USD_Billion']]

    return df
transform(df, csv_path)
log_progress("Data transformation complete. Initiating Loading process")

def load_to_csv(df, output_path):
    df.to_csv(output_path)
load_to_csv(df, output_path)
log_progress("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    conn = sqlite3.connect(sql_connection,)
    log_progress("SQL Connection initiated")
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

def run_query(query_statement, sql_connection):
    conn = sqlite3.connect(sql_connection)
    df = pd.read_sql(query_statement, conn)
    conn.close()
    print(df)
run_query(query_statement, sql_connection)
log_progress("Process Complete")
log_progress("Server Connection closed")
