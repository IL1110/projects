

import requests
import json

url = "https://coronavirus.m.pipedream.net/"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

#print(response.text)

covid_json = json.loads(response.text)

for item in covid_json['rawData']:
    if item['Country_Region'] == 'Russia':
        print (item)
