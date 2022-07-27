
from hashlib import new
import jsonlines
import json
import pandas as pd
import datetime as dt

import numpy as np
from random import randint


# with jsonlines.open('data.json') as f:
#     for line in f.iter():
#       data = line
#       #print(data['LogDateUTC'])

# item_dict = json.loads(data['LogDateUTC'])
# print("######", len(item_dict['result'][0]['run']))


with open('data.json', encoding='utf-8-sig') as f:
    lines = f.read().splitlines()

df_inter = pd.DataFrame(lines)
df_inter.columns = ['json_element']
df_inter['json_element'].apply(json.loads)


df_final = pd.json_normalize(df_inter['json_element'].apply(json.loads))
print(df_final)
print(df_final['LogDateUTC'])
print("We find this amount of lines : ", len(df_final.index))

df_final['LogDateUTC'] = pd.to_datetime(df_final['LogDateUTC'])
dataFrame = pd.DataFrame(df_final['LogDateUTC'])
resDF = dataFrame.loc[dataFrame['LogDateUTC'] > "2020"]
df = resDF.groupby([dataFrame['LogDateUTC'].dt.to_period('H')]).count().unstack()
print(df)
#unique = df_final.loc[df_final['MGuidR']].agg(['nunique','count','size'])
#print("WIUUUUUU", unique)
print("Here you find the unique Id of MGuidR", df_final['MGuidR'].nunique())



df_hourly_id = pd.json_normalize(df_inter['json_element'].apply(json.loads))
#df = pd.DataFrame({'LogDateUTC', df_final['MGuidR'].nunique()})

#This prints the two columns needed
print(df_hourly_id[['MGuidR' , 'LogDateUTC']])
df_hourly_id['LogDateUTC'] = pd.to_datetime(df_final['LogDateUTC'])
dataFrame = pd.DataFrame(df_hourly_id['LogDateUTC'])
print(dataFrame)

#This outputs the unique Id's of MGuidR per hour
new_data = df_hourly_id.groupby(dataFrame['LogDateUTC'].dt.to_period('H'))['MGuidR'].nunique()
print("YES?", new_data)


#Here we find out what MGuidR generated the most timeOn
