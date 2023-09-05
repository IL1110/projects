#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
import os
import sys
import pyspark.sql.types as sqlt

from pyspark.sql import SQLContext
import pyspark.sql.types as sqlt
import pyspark.sql.functions as sqlf
from pyspark import SparkConf
try:
    sc.stop()
except:
    pass
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#HDFS_MASTER = 'hadoop-master'
conf = SparkConf()
conf.setMaster('yarn')
conf.setAppName('spark-test')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext
sql = spark.sql


import json
import requests
import pandas as pd


# In[2]:


url = "https://disease.sh/v3/covid-19/countries"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)
diseas_json = json.loads(response.text)

dfpd = pd.DataFrame(diseas_json)

df_long = pd.DataFrame({'long':[]})
for i in range(0, len(dfpd['countryInfo'])):
    df_long.loc[i] = dfpd['countryInfo'][i]['long']

df_lat = pd.DataFrame({'lat':[]})
for i in range(0, len(dfpd['countryInfo'])):
    df_lat.loc[i] = dfpd['countryInfo'][i]['lat']
    
df_dis = dfpd[['updated',
         'country',
         'cases',
         'deaths',
         'recovered',
         'active',
         'tests',
         'population'
          ]]

    
df_dis['lat'] = df_lat['lat']
df_dis['long'] = df_long['long']


# In[3]:


df = spark.createDataFrame(df_dis)
df.write.mode('overwrite').format('parquet').saveAsTable('saulov_ilia.covid')


# In[ ]:




