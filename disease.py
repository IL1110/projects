import json
import pandas as pd
import psycopg2
import requests
import sqlalchemy
from sqlalchemy import create_engine

print('
url = "https://disease.sh/v3/covid-19/countries"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

diseas_json = json.loads(response.text)

df = pd.DataFrame(diseas_json)

df_long=pd.DataFrame({'long':[]})
for i in range(0, len(df['countryInfo'])):
    df_long.loc[i] =df['countryInfo'][i]['long']

df_lat=pd.DataFrame({'lat':[]})
for i in range(0, len(df['countryInfo'])):
    df_lat.loc[i] =df['countryInfo'][i]['lat']

df_dis = df[['updated',
         'country',
         'cases',
         'deaths',
         'recovered',
         'active',
         'tests',
         'population',
          ]]

df_dis['lat'] = df_lat['lat']
df_dis['long'] = df_long['long']

# with pd.option_context('display.max_columns', None):
#     print(df_dis)


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

df_dis['updated'] = pd.to_datetime(df_dis['updated'], unit='ms')


df_dis.to_sql(
    'general_diseas',
    con = conn,
    if_exists = 'replace',
    dtype = {
         'updated':sqlalchemy.types.DATE,
         'country':sqlalchemy.types.String,
         'deaths':sqlalchemy.types.BIGINT,
         'recovered':sqlalchemy.types.BIGINT,
         'active':sqlalchemy.types.BIGINT,
         'tests':sqlalchemy.types.BIGINT,
         'population':sqlalchemy.types.BIGINT,
         'lat': sqlalchemy.types.NUMERIC,
         'long': sqlalchemy.types.NUMERIC
        }
    )
        ')