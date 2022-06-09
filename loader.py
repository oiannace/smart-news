#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import psycopg2
import random
from pandasql import sqldf
import sqlalchemy
import tweet_extraction
import pandas as pd

'''
load process:
    - configure the data into dataframes based on the created dimensional model
    - insert dataframes into respective postgresql database
'''

i = 0
surrogate_key_list = random.sample(range(10000, 99999), 1000)

tweets_lemm_df, user_df = tweet_extraction.load_data()

user_date_dim = pd.DataFrame(user_df['Creation_Date'].drop_duplicates().reset_index(drop=True))
date_cols = ['Year' , 'Month' , 'Day' , 'Hour' , 'Minute' , 'Second' ]
indices = [0,5,8,11,14,17,20]
for k in range(len(date_cols)):
    user_date_dim[date_cols[k]] = user_date_dim['Creation_Date'].str[indices[k]:indices[k+1]-1]

user_date_dim.loc[:, 'date_key'] = 0

for j in range(len(user_date_dim['date_key'])):
     user_date_dim.loc[j,'date_key'] = surrogate_key_list[i]
     i+=1
 
print(user_date_dim)

tweet_date_dim = pd.DataFrame(tweets_lemm_df['Tweet_Date'].drop_duplicates().reset_index(drop=True))
for k in range(len(date_cols)):
    tweet_date_dim[date_cols[k]] = tweet_date_dim['Tweet_Date'].str[indices[k]:indices[k+1]-1]

tweet_date_dim.loc[:, 'date_key'] = 0

for j in range(len(tweet_date_dim['date_key'])):
     tweet_date_dim.loc[j,'date_key'] = surrogate_key_list[i]
     i+=1
 
print(tweet_date_dim)

