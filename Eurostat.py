from pyjstat import pyjstat
import requests
import pandas as pd
import os
os.makedirs('data', exist_ok=True)
rename_dict = {
    r'^Euro area \(.*\)$': 'Euro area',
    r'^European Union \(.*\)$': 'EU27'
}

#Euro Area Flash Estimate
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?lang=en&lastTimePeriod=61&coicop=CP00&geo=EA&geo=BE&geo=DE&geo=EE&geo=IE&geo=EL&geo=ES&geo=FR&geo=HR&geo=IT&geo=CY&geo=LV&geo=LT&geo=LU&geo=MT&geo=NL&geo=AT&geo=PT&geo=SI&geo=SK&geo=FI')
df = dataset.write('dataframe')
df.replace(rename_dict, regex=True, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.to_csv('data/Eurostat_Inflation_EA_HICP_Time.csv', index=True)

#Europe Latest Data
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?lang=en&lastTimePeriod=61&coicop=CP00&geo=EU&geo=EA&geo=BE&geo=BG&geo=CZ&geo=DK&geo=DE&geo=EE&geo=IE&geo=EL&geo=ES&geo=FR&geo=HR&geo=IT&geo=CY&geo=LV&geo=LT&geo=LU&geo=HU&geo=MT&geo=NL&geo=AT&geo=PL&geo=PT&geo=RO&geo=SI&geo=SK&geo=FI&geo=SE&geo=NO&geo=IS&geo=CH')
df = dataset.write('dataframe')
df.replace(rename_dict, regex=True, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.to_csv('data/Eurostat_Inflation_All_HICP_Time.csv', index=True)

#EU27 Main Components
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?lang=en&lastTimePeriod=61&coicop=CP00&geo=EU&coicop=FOOD&coicop=IGD_NNRG&coicop=NRG&coicop=SERV')
df = dataset.write('dataframe')
df.replace(rename_dict, regex=True, inplace=True)
df_new = df.pivot(index='Classification of individual consumption by purpose (COICOP)', columns='Time', values='value')
df_new.to_csv('data/Eurostat_Inflation_EU27_Components_Time.csv', index=True)

#EA Main Components
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?lang=en&lastTimePeriod=61&coicop=CP00&geo=EA&coicop=FOOD&coicop=IGD_NNRG&coicop=NRG&coicop=SERV')
df = dataset.write('dataframe')
df.replace(rename_dict, regex=True, inplace=True)
df_new = df.pivot(index='Classification of individual consumption by purpose (COICOP)', columns='Time', values='value')
df_new.to_csv('data/Eurostat_Inflation_EA_Components_Time.csv', index=True)

#Europe Food 
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?lang=en&lastTimePeriod=61&coicop=FOOD&geo=EU&geo=EA&geo=BE&geo=BG&geo=CZ&geo=DK&geo=DE&geo=EE&geo=IE&geo=EL&geo=ES&geo=FR&geo=HR&geo=IT&geo=CY&geo=LV&geo=LT&geo=LU&geo=HU&geo=MT&geo=NL&geo=AT&geo=PL&geo=PT&geo=RO&geo=SI&geo=SK&geo=FI&geo=SE&geo=NO&geo=IS&geo=CH')
df = dataset.write('dataframe')
df.replace(rename_dict, regex=True, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.to_csv('data/Eurostat_Inflation_All_Food_Time.csv', index=True)

#Europe Services 
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?lang=en&lastTimePeriod=61&coicop=SERV&geo=EU&geo=EA&geo=BE&geo=BG&geo=CZ&geo=DK&geo=DE&geo=EE&geo=IE&geo=EL&geo=ES&geo=FR&geo=HR&geo=IT&geo=CY&geo=LV&geo=LT&geo=LU&geo=HU&geo=MT&geo=NL&geo=AT&geo=PL&geo=PT&geo=RO&geo=SI&geo=SK&geo=FI&geo=SE&geo=NO&geo=IS&geo=CH')
df = dataset.write('dataframe')
df.replace(rename_dict, regex=True, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.to_csv('data/Eurostat_Inflation_All_Services_Time.csv', index=True)

#Europe Energy 
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?lang=en&lastTimePeriod=61&coicop=NRG&geo=EU&geo=EA&geo=BE&geo=BG&geo=CZ&geo=DK&geo=DE&geo=EE&geo=IE&geo=EL&geo=ES&geo=FR&geo=HR&geo=IT&geo=CY&geo=LV&geo=LT&geo=LU&geo=HU&geo=MT&geo=NL&geo=AT&geo=PL&geo=PT&geo=RO&geo=SI&geo=SK&geo=FI&geo=SE&geo=NO&geo=IS&geo=CH')
df = dataset.write('dataframe')
df.replace(rename_dict, regex=True, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.to_csv('data/Eurostat_Inflation_All_Energy_Time.csv', index=True)

#Europe Non-energy industrial goods 
dataset = pyjstat.Dataset.read('https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_manr?lang=en&lastTimePeriod=61&coicop=IGD_NNRG&geo=EU&geo=EA&geo=BE&geo=BG&geo=CZ&geo=DK&geo=DE&geo=EE&geo=IE&geo=EL&geo=ES&geo=FR&geo=HR&geo=IT&geo=CY&geo=LV&geo=LT&geo=LU&geo=HU&geo=MT&geo=NL&geo=AT&geo=PL&geo=PT&geo=RO&geo=SI&geo=SK&geo=FI&geo=SE&geo=NO&geo=IS&geo=CH')
df = dataset.write('dataframe')
df.replace(rename_dict, regex=True, inplace=True)
df_new = df.pivot(index='Geopolitical entity (reporting)', columns='Time', values='value')
df_new.to_csv('data/Eurostat_Inflation_All_Non_Energy_Goods_Time.csv', index=True)
