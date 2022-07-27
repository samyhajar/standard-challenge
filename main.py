
from hashlib import new
import jsonlines
import json
import pandas as pd
import datetime as dt

import numpy as np
from numpy import array
from random import randint


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
print("Here you find the unique Id'S of MGuidR", df_final['MGuidR'].nunique())



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

#print all unique ID of MGuidR
#print("#################", df_hourly_id['MGuidR'])
unique_ID = pd.unique(df_hourly_id['MGuidR'])



#Here we find out what MGuidR generated the most timeOn
df_final = pd.json_normalize(df_inter['json_element'].apply(json.loads))
# df2 = df_final.query('timeOnPageMS == timeOnPageMS.max()')
# main = df2.index[0]
# value = df_final.loc[main].at["MGuidR"]
# print("Here is the MguidR that generated the most timeInPageMS : ",value)
# sum = df_final.groupby('timeOnPageMS').sum()
# print(sum)

# filt = df_final['MguidR'] == df_final['MguidR']
# df_final[filt].sum(axis=0, numeric_only=True) == 89


#grouped_df = df_final.groupby([ "timeOnPageMS" ,"MGuidR"])
grouped_df = df_final[['timeOnPageMS', 'MGuidR']]
print("CHECKOUT", grouped_df)

total = df_final.groupby(['MGuidR', 'LogDay'])['timeOnPageMS'].sum()
print("CHECKIN", total)

max_value_column = total.max()
print(max_value_column)



# print(grouped_and_summed)






#df2['SubscriptionName'] = df2['Sum'].gt(20).map({True: 'Pro', False: 'Beginner'})



#print(df2)
#print ("Maximum value of column ", col, " and its corresponding row values:\n", max_x)


#maxValues = df_final['timeOnPageMS'].groupby(df_final['timeOnPageMS']).max()
#print(maxValues)

#hihi = df_final.loc[df_final['timeOnPageMS'].eq(df.groupby('MGuidR').df_final['timeOnPageMS'].transform('max'))]

#df_final['max_entry'] = df_final.groupby(['Contract','Ref_Date'])['Difference'].transform(lambda x: np.where(x == x.max(), 'x', ' '))
#print(hihi)
