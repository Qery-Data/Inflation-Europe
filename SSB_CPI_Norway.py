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

#CPI Yearly Level 3
ssburl = 'https://data.ssb.no/api/v0/no/table/03013/'
query = {
  "query": [
    {
      "code": "Konsumgrp",
      "selection": {
        "filter": "vs:CoiCop2016niva3",
        "values": [
          "01.1",
          "01.2",
          "02.1",
          "02.2",
          "03.1",
          "03.2",
          "04.1",
          "04.2",
          "04.3",
          "04.4",
          "04.5",
          "05.1",
          "05.2",
          "05.3",
          "05.4",
          "05.5",
          "05.6",
          "06.1",
          "06.2",
          "07.1",
          "07.2",
          "07.3",
          "08.1",
          "08.2",
          "08.3",
          "09.1",
          "09.2",
          "09.3",
          "09.4",
          "09.5",
          "09.6",
          "11.1",
          "11.2",
          "12.1",
          "12.3",
          "12.4",
          "12.5",
          "12.6",
          "12.7"
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
        "values": ["120"
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
df_new.to_csv('data/SSB_CPI_Groups_12M_Last10y.csv', index=True)

#CPI Yearly Level 4
ssburl = 'https://data.ssb.no/api/v0/no/table/03013/'
query = {
  "query": [
    {
      "code": "Konsumgrp",
      "selection": {
        "filter": "vs:CoiCop2016niva4",
        "values": [
          "01.1.1",
          "01.1.2",
          "01.1.3",
          "01.1.4",
          "01.1.5",
          "01.1.6",
          "01.1.7",
          "01.1.8",
          "01.1.9",
          "01.2.1",
          "01.2.2",
          "02.1.1",
          "02.1.2",
          "02.1.3",
          "02.2.0",
          "03.1.1",
          "03.1.2",
          "03.1.3",
          "03.1.4",
          "03.2.1",
          "03.2.2",
          "04.1.1",
          "04.1.2",
          "04.2.1",
          "04.2.2",
          "04.3.1",
          "04.3.2",
          "04.4.0",
          "04.5.1",
          "04.5.3",
          "04.5.4",
          "04.5.5",
          "05.1.1",
          "05.1.2",
          "05.2.0",
          "05.3.1",
          "05.3.2",
          "05.3.3",
          "05.4.0",
          "05.5.1",
          "05.5.2",
          "05.6.1",
          "05.6.2",
          "06.1.1",
          "06.1.2",
          "06.1.3",
          "06.2.1",
          "06.2.2",
          "06.2.3",
          "07.1.1",
          "07.1.2",
          "07.1.3",
          "07.2.1",
          "07.2.2",
          "07.2.3",
          "07.2.4",
          "07.3.1",
          "07.3.2",
          "07.3.3",
          "07.3.4",
          "08.1.0",
          "08.2.0",
          "08.3.0",
          "09.1.1",
          "09.1.2",
          "09.1.3",
          "09.1.4",
          "09.1.5",
          "09.2.1",
          "09.2.2",
          "09.3.1",
          "09.3.2",
          "09.3.3",
          "09.3.4",
          "09.4.1",
          "09.4.2",
          "09.5.1",
          "09.5.2",
          "09.5.4",
          "09.6.0",
          "11.1.1",
          "11.1.2",
          "11.2.0",
          "12.1.1",
          "12.1.2",
          "12.1.3",
          "12.3.1",
          "12.3.2",
          "12.4.0",
          "12.5.2",
          "12.5.4",
          "12.6.2",
          "12.7.0"
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
        "values": ["120"
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
df_new = df_new.dropna(subset=[df_new.columns[-1]])
df_new.to_csv('data/SSB_CPI_Sub-Groups1_12M_Last10y.csv', index=True)

#CPI Yearly Level 5
ssburl = 'https://data.ssb.no/api/v0/no/table/03013/'
query = {
  "query": [
    {
      "code": "Konsumgrp",
      "selection": {
        "filter": "vs:CoiCop2016niva5",
        "values": [
          "01.1.1.1",
          "01.1.1.2",
          "01.1.1.3",
          "01.1.1.4",
          "01.1.1.5",
          "01.1.1.6",
          "01.1.1.7",
          "01.1.1.8",
          "01.1.2.1",
          "01.1.2.2",
          "01.1.2.3",
          "01.1.2.4",
          "01.1.2.5",
          "01.1.2.6",
          "01.1.2.7",
          "01.1.2.8",
          "01.1.3.1",
          "01.1.3.2",
          "01.1.3.4",
          "01.1.3.5",
          "01.1.3.6",
          "01.1.4.1",
          "01.1.4.2",
          "01.1.4.4",
          "01.1.4.5",
          "01.1.4.6",
          "01.1.4.7",
          "01.1.5.1",
          "01.1.5.2",
          "01.1.5.3",
          "01.1.5.4",
          "01.1.6.1",
          "01.1.6.2",
          "01.1.6.3",
          "01.1.6.4",
          "01.1.7.1",
          "01.1.7.2",
          "01.1.7.3",
          "01.1.7.4",
          "01.1.7.5",
          "01.1.8.1",
          "01.1.8.2",
          "01.1.8.3",
          "01.1.8.4",
          "01.1.8.5",
          "01.1.8.6",
          "01.1.9.1",
          "01.1.9.2",
          "01.1.9.3",
          "01.1.9.4",
          "01.1.9.9",
          "01.2.1.1",
          "01.2.1.2",
          "01.2.1.3",
          "01.2.2.1",
          "01.2.2.2",
          "01.2.2.3",
          "02.1.1.1",
          "02.1.1.2",
          "02.1.2.1",
          "02.1.2.3",
          "02.1.3.1",
          "02.2.0.1",
          "02.2.0.2",
          "02.2.0.3",
          "05.6.1.1",
          "07.2.2.1",
          "07.2.2.2",
          "07.2.2.3"
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
        "values": ["120"
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
df_new = df_new.dropna(subset=[df_new.columns[-1]])
df_new.to_csv('data/SSB_CPI_Sub-Groups2_12M_Last10y.csv', index=True)

#CPI Yearly Level 6
ssburl = 'https://data.ssb.no/api/v0/no/table/03013/'
query = {
  "query": [
    {
      "code": "Konsumgrp",
      "selection": {
        "filter": "vs:CoiCop2016niva6",
        "values": [
          "01.1.1.1_11111",
          "01.1.1.2_11121",
          "01.1.1.2_11122",
          "01.1.1.3_11131",
          "01.1.1.3_11132",
          "01.1.1.4_11141",
          "01.1.1.4_11142",
          "01.1.1.4_11143",
          "01.1.1.4_11144",
          "01.1.1.4_11145",
          "01.1.1.4_11146",
          "01.1.1.4_11147",
          "01.1.1.5_11151",
          "01.1.1.6_11161",
          "01.1.1.6_11162",
          "01.1.1.7_11171",
          "01.1.1.8_11181",
          "01.1.2.1_11211",
          "01.1.2.2_11221",
          "01.1.2.3_11231",
          "01.1.2.4_11241",
          "01.1.2.5_11251",
          "01.1.2.6_11261",
          "01.1.2.7_11271",
          "01.1.2.7_11272",
          "01.1.2.7_11273",
          "01.1.2.7_11274",
          "01.1.2.8_11281",
          "01.1.2.8_11282",
          "01.1.2.8_11283",
          "01.1.2.8_11289",
          "01.1.3.1_11311",
          "01.1.3.1_11312",
          "01.1.3.1_11319",
          "01.1.3.2_11321",
          "01.1.3.2_11322",
          "01.1.3.2_11329",
          "01.1.3.4_11341",
          "01.1.3.5_11351",
          "01.1.3.6_11361",
          "01.1.3.6_11362",
          "01.1.3.6_11363",
          "01.1.3.6_11369",
          "01.1.4.1_11411",
          "01.1.4.2_11421",
          "01.1.4.4_11441",
          "01.1.4.5_11451",
          "01.1.4.6_11461",
          "01.1.4.6_11462",
          "01.1.4.6_11463",
          "01.1.4.6_11464",
          "01.1.4.7_11471",
          "01.1.5.1_11511",
          "01.1.5.2_11521",
          "01.1.5.3_11531",
          "01.1.5.4_11541",
          "01.1.6.1_11611",
          "01.1.6.1_11612",
          "01.1.6.1_11613",
          "01.1.6.1_11614",
          "01.1.6.1_11615",
          "01.1.6.1_11616",
          "01.1.6.1_11617",
          "01.1.6.1_11619",
          "01.1.6.2_11621",
          "01.1.6.3_11631",
          "01.1.6.3_11632",
          "01.1.6.4_11641",
          "01.1.7.1_11711",
          "01.1.7.1_11712",
          "01.1.7.1_11713",
          "01.1.7.1_11714",
          "01.1.7.1_11715",
          "01.1.7.1_11719",
          "01.1.7.2_11721",
          "01.1.7.3_11731",
          "01.1.7.3_11732",
          "01.1.7.3_11733",
          "01.1.7.3_11739",
          "01.1.7.4_11741",
          "01.1.7.4_11742",
          "01.1.7.5_11751",
          "01.1.8.1_11811",
          "01.1.8.1_11812",
          "01.1.8.2_11821",
          "01.1.8.2_11822",
          "01.1.8.2_11823",
          "01.1.8.3_11831",
          "01.1.8.3_11832",
          "01.1.8.3_11833",
          "01.1.8.3_11834",
          "01.1.8.4_11841",
          "01.1.8.4_11842",
          "01.1.8.4_11843",
          "01.1.8.5_11851",
          "01.1.8.6_11861",
          "01.1.9.1_11911",
          "01.1.9.1_11912",
          "01.1.9.1_11913",
          "01.1.9.1_11914",
          "01.1.9.1_11915",
          "01.1.9.2_11921",
          "01.1.9.3_11931",
          "01.1.9.4_11941",
          "01.1.9.4_11942",
          "01.1.9.4_11943",
          "01.1.9.9_11991",
          "01.1.9.9_11992",
          "01.1.9.9_11993",
          "01.1.9.9_11994",
          "01.2.1.1_12111",
          "01.2.1.2_12121",
          "01.2.1.3_12131",
          "01.2.2.1_12211",
          "01.2.2.2_12221",
          "01.2.2.3_12231",
          "01.2.2.3_12232",
          "02.2.0.1_22011",
          "04.5.3.0_00772",
          "05.6.1.2_00149",
          "07.2.2.1_72211",
          "07.2.2.2_72221",
          "07.2.2.3_01497",
          "12.1.3.2_00131",
          "12.1.3.2_00152"
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
        "values": ["120"
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
df_new = df_new.dropna(subset=[df_new.columns[-1]])
df_new.to_csv('data/SSB_CPI_Items_Item_Groups_12M_Last10y.csv', index=True)