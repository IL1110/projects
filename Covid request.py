import json
import pandas as pd
import psycopg2
import requests
import sqlalchemy
from sqlalchemy import create_engine


url = "https://coronavirus.m.pipedream.net/"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
covid_json = json.loads(response.text)

df = pd.DataFrame(covid_json['rawData'])
df = df[['Province_State',
         'Country_Region',
         'Last_Update',
         'Confirmed',
         'Deaths',
         'Recovered',
         'Active',
         'Lat',
         'Long_',
         'Incident_Rate',
         'Case_Fatality_Ratio']]

with pd.option_context('display.max_columns', None):
    print(df)



#!введите свои реквизиты!
# DB_HOST = '87.242.126.7'
# DB_USER = 'student16'
# DB_USER_PASSWORD = 'student16_password'
# DB_NAME = 'covid_project16'
#
# conn_psql = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_USER_PASSWORD, dbname=DB_NAME)

# conn_sqlite = sqlite3.connect('covid_project16.db')
# c = conn_sqlite.cursor()

# from pandas.api.types import is_string_dtype
# from pandas.api.types import is_numeric_dtype
# print(is_string_dtype(df['Province_State']))

conn_string = 'postgresql://student16:student16_password@87.242.126.7/covid_project16'

db = create_engine(conn_string)
conn = db.connect()
conn1 = psycopg2.connect(
    database="covid_project16",
    user='student16',
    password='student16_password',
    host='87.242.126.7',
    port='5432'
)
conn1.autocommit = True
cursor = conn1.cursor()

df.to_sql('general', conn, if_exists= 'replace')

# df.to_sql(
#     'general',
#     con = conn_psql,
#     if_exists = 'replace'
#     ,dtype = {
#         'Province_State': sqlalchemy.types.String,
#         'Country_Region': sqlalchemy.types.String,
#         'Last_Update': sqlalchemy.types.TIMESTAMP,
#         'Confirmed': sqlalchemy.types.BIGINT,
#         'Deaths': sqlalchemy.types.BIGINT,
#         'Recovered': sqlalchemy.types.BIGINT,
#         'Active': sqlalchemy.types.BIGINT,
#         'Incident_Rate': sqlalchemy.types.NUMERIC,
#         'Case_Fatality_Ratio': sqlalchemy.types.NUMERIC}
#     )
