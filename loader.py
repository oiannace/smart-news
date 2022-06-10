#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import psycopg2
import random
from pandasql import sqldf
import sqlalchemy
import tweet_extraction
import pandas as pd
import json

'''
load process:
    - configure the data into dataframes based on the created dimensional model
    - insert dataframes into respective postgresql database
'''

i = 0
surrogate_key_list = random.sample(range(10000, 99999), 1000)

tweets_lemm_df, user_df = tweet_extraction.load_data()


#transforming user_date_dim table into appropriate form for snowflake schema model
user_date_dim = pd.DataFrame(user_df['creation_date'].drop_duplicates().reset_index(drop=True))
date_cols = ['year' , 'month' , 'day' , 'hour' , 'minute' , 'second' ]
indices = [0,5,8,11,14,17,20]
for k in range(len(date_cols)):
    user_date_dim[date_cols[k]] = user_date_dim['creation_date'].str[indices[k]:indices[k+1]-1]

user_date_dim.loc[:, 'date_key'] = 0

for j in range(len(user_date_dim['date_key'])):
     user_date_dim.loc[j,'date_key'] = surrogate_key_list[i]
     i+=1
 

#transforming tweet_date_dim table into appropriate form for snowflake schema model
tweet_date_dim = pd.DataFrame(tweets_lemm_df['tweet_date'].drop_duplicates().reset_index(drop=True))
for k in range(len(date_cols)):
    tweet_date_dim[date_cols[k]] = tweet_date_dim['tweet_date'].str[indices[k]:indices[k+1]-1]

tweet_date_dim.loc[:, 'date_key'] = 0

for j in range(len(tweet_date_dim['date_key'])):
     tweet_date_dim.loc[j,'date_key'] = surrogate_key_list[i]
     i+=1
 

#transforming user_loc_dim table into appropriate form for snowflake schema model
user_loc_dim = pd.DataFrame(user_df['location'].drop_duplicates().reset_index(drop=True))

user_loc_dim.loc[:, 'location_key'] = 0

for j in range(len(user_loc_dim['location_key'])):
     user_loc_dim.loc[j,'location_key'] = surrogate_key_list[i]
     i+=1
 


#transforming user_dim table into appropriate form for snowflake schema model
user_dim = pd.DataFrame(user_df[['user_id', 'username', 'name', 'bio']].drop_duplicates().reset_index(drop=True))

user_dim.loc[:, 'user_key'] = 0

for j in range(len(user_dim['user_key'])):
    user_dim.loc[j,'user_key'] = surrogate_key_list[i]
    i+=1


#creating a table with all info in order to query the tables with surrogate keys effectively
complete_table = tweets_lemm_df.merge(user_df, how='left', left_on='user_id', right_on='user_id')

#transforming tweet content from a list of strings to a string, can be reversed with json.loads()
complete_table['tweet_content'] = complete_table['tweet_content'].apply(lambda row: json.dumps(row))


user_dim_query = '''
SELECT DISTINCT ud.user_key,
        uld.location_key,
        udd.date_key as creation_date_key,
        ct.user_id,
        ct.username,
        ct.name,
        ct.bio
        
FROM complete_table ct

INNER JOIN user_date_dim udd
ON udd.creation_date = ct.creation_date

INNER JOIN user_dim ud
ON ct.user_id = ud.user_id

INNER JOIN user_loc_dim uld
ON ct.location = uld.location
'''

user_dim = sqldf(user_dim_query)



fact_table_query = '''
    SELECT udd.date_key AS user_date_key, 
           tdd.date_key AS tweet_date_key,
           uld.location_key, 
           ud.user_key, 
           ct.tweet_id, 
           ct.tweet_content
    
    From complete_table ct
    
    INNER JOIN tweet_date_dim tdd
    ON ct.tweet_date = tdd.tweet_date
    
    INNER JOIN user_dim ud
    ON ct.user_id = ud.user_id
    
    INNER JOIN user_date_dim udd
    ON ct.creation_date = udd.creation_date
    
    INNER JOIN user_loc_dim uld
    ON ct.location = uld.location
'''

tweet_content_fact = sqldf(fact_table_query)


user_date_dim = user_date_dim[['date_key', 'year', 'month', 'day', 'hour', 'minute']]
tweet_date_dim = tweet_date_dim[['date_key', 'year', 'month', 'day', 'hour', 'minute']]
user_loc_dim = user_loc_dim[['location_key', 'location']]


username = "your username"
password = "your password"
#USERNAME and PASSWORD are specific to your postgreSQL account
engine = sqlalchemy.create_engine(f"postgresql://{username}:{password}@localhost/tweet_data_db")

tables = [user_date_dim, tweet_date_dim, user_loc_dim, user_dim, tweet_content_fact]
table_names = ["user_date_dim", "tweet_date_dim", "location_dim", "user_dim", "tweet_details_fact"]
for i in range(len(tables)):
    tables[i].to_sql(name=table_names[i], con=engine, schema="processed_tweets", if_exists="append", index=False)
