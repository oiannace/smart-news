import json
import psycopg2
from lib import loading_util_functions
import sqlalchemy
import os
import boto3
import pandas as pd 

def lambda_handler(event, context):
    
    bucket_name = 'tweet-bucket'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    file_names = ["user_date_dim.csv", "tweet_date_dim.csv", "user_loc_dim.csv", "user_dim.csv", "tweet_content_fact.csv"]
    for file in file_names:
        response = bucket.download_file(file, f"/tmp/{file}")
    
    user_dim = pd.read_csv("/tmp/user_dim.csv")
    user_dim.drop('Unnamed: 0', axis=1, inplace=True)
    
    
    user_date_dim = pd.read_csv("/tmp/user_date_dim.csv")
    user_date_dim.drop('Unnamed: 0', axis=1, inplace=True)
    
    user_loc_dim = pd.read_csv("/tmp/user_loc_dim.csv")
    user_loc_dim.drop('Unnamed: 0', axis=1, inplace=True)
    
    tweet_date_dim = pd.read_csv("/tmp/tweet_date_dim.csv")
    tweet_date_dim.drop('Unnamed: 0', axis=1, inplace=True)
    
    tweet_content_fact = pd.read_csv("/tmp/tweet_content_fact.csv")
    tweet_content_fact.drop('Unnamed: 0', axis=1, inplace=True)
    
    print(user_date_dim.columns, user_loc_dim.columns, tweet_content_fact.columns, tweet_date_dim.columns)
    
    tables = [user_date_dim, tweet_date_dim, user_loc_dim, user_dim, tweet_content_fact]
    table_names = ["user_date_dim", "tweet_date_dim", "location_dim", "user_dim", "tweet_details_fact"]
    credentials = {'username' : os.environ['db_username'],
                    'password' : os.environ['db_pass'],
                    'host' : os.environ['db_host'],
                    'port' : '5432',
                    'schema' : os.environ['db_schema'],
                    'db_name' : os.environ['db_name']}
    
    
    queries = loading_util_functions.queries_func()
    loading_util_functions.create_tables(queries, credentials)
    
    for i in range(len(tables)):
        loading_util_functions.data_load(tables[i], table_names[i], credentials)
