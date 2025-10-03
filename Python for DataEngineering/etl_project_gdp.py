import pandas as pd 
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

# python3.11 -m pip install pandas
# python3.11 -m pip install bs4

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = '#mw-content-text table.wikitable.sortable tbody tr'
csv_path = '/home/project/Countries_by_GDP.csv'
sql_connection = 'World_Economies.db'
table_name = 'Countries_by_GDP'
query_statement = 'SELECT * FROM Countries_by_GDP WHERE GDP_Billion > 1000'
message = 'Log retrieved.'
log_file = "log_file.txt" 

def extract(url, table_attribs):

    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    all_rows = soup.select(table_attribs)

    df_list = []

    for i in all_rows[3:]:
        cells = i.find_all(['td', 'th'])

        if len(cells) >=8:
            country = cells[0].get_text(strip=True)
            gdp = cells[2].get_text(strip=True)

            df_list.append([country, gdp])
        
    df = pd.DataFrame(df_list, columns = ['Country', 'GDP_Billion'])
    return df
df = extract(url, table_attribs)

def transform(df):
    df['GDP_Billion'] = df['GDP_Billion'].str.replace(',', '')
    df['GDP_Billion'] = pd.to_numeric(df['GDP_Billion'])
    df['GDP_Billion'] = round(df.GDP_Billion * 0.01, 0)
    return df
df = transform(df)

def load_csv(df, csv_path):
    df.to_csv(csv_path)
    print('"gdpdata.csv has been stored"')
load_csv(df, csv_path)

def load_to_db(df, sql_connection, table_name):
    conn = sqlite3.connect(sql_connection,)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
load_to_db(df, sql_connection, table_name)

def run_query(query_statement, sql_connection):
    conn = sqlite3.connect(sql_connection)
    df = pd.read_sql(query_statement, conn)
    conn.close()
    print(df.head())
run_query(query_statement, sql_connection)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 
log_progress(message)