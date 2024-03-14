from pyjstat import pyjstat
import requests
import os
import pandas as pd
os.makedirs('data', exist_ok=True)

#CPI Total Level 1
ssburl = 'https://data.ssb.no/api/v0/no/table/03013/'
query = {
  "query": [
    {
      "code": "Konsumgrp",
      "selection": {
        "filter": "vs:CoiCop2016niva1",
        "values": [
          "TOTAL"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Tolvmanedersendring"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='statistikkvariabel', columns='måned', values='value')
df_new.to_csv('data/SSB_CPI_Total.csv', index=True)

#CPI Yearly Change Level 2
ssburl = 'https://data.ssb.no/api/v0/no/table/03013/'
query = {
  "query": [
    {
      "code": "Konsumgrp",
      "selection": {
        "filter": "vs:CoiCop2016niva2",
        "values": [
          "01",
          "02",
          "03",
          "04",
          "05",
          "06",
          "07",
          "08",
          "09",
          "10",
          "11",
          "12"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Tolvmanedersendring"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          "1"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='statistikkvariabel', columns='konsumgruppe', values='value')
df_new.to_csv('data/SSB_CPI_Divisions_12M_Latest.csv', index=True)

#CPI Monthly Change Level 2
ssburl = 'https://data.ssb.no/api/v0/no/table/03013/'
query = {
  "query": [
    {
      "code": "Konsumgrp",
      "selection": {
        "filter": "vs:CoiCop2016niva2",
        "values": [
          "01",
          "02",
          "03",
          "04",
          "05",
          "06",
          "07",
          "08",
          "09",
          "10",
          "11",
          "12"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Manedsendring"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          "1"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='statistikkvariabel', columns='konsumgruppe', values='value')
df_new.to_csv('data/SSB_CPI_Divisions_Monthly_Latest.csv', index=True)

#CPI Weights
ssburl = 'https://data.ssb.no/api/v0/no/table/03013/'
query = {
  "query": [
    {
      "code": "Konsumgrp",
      "selection": {
        "filter": "vs:CoiCop2016niva2",
        "values": [
          "01",
          "02",
          "03",
          "04",
          "05",
          "06",
          "07",
          "08",
          "09",
          "10",
          "11",
          "12"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "KpiVektMnd"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "1980M02",
          "2000M02",
          "2024M02"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
df = dataset.write('dataframe')
df_new = df.pivot(index='konsumgruppe', columns='måned', values='value')
df_new.to_csv('data/SSB_CPI_Weights_Latest.csv', index=True)