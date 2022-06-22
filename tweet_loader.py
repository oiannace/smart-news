
import json
import psycopg2
from lib import create_star_schema, loader
import sqlalchemy
import os
import boto3

def lambda_handler(event, context):
    
    bucket_name = 'pre-transformation-bucket'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    file_names = ["user_date_dim.csv", "tweet_date_dim.csv", "user_loc_dim.csv", "user_dim.csv", "tweet_content_fact.csv"]
    for file in file_names:
        print('here')
        response = bucket.download_file(file, f"/tmp/{file}")
    
    user_dim = open("/tmp/user_dim.csv", 'r').read()
    print(type(user_dim))
    
    tables = [user_date_dim, tweet_date_dim, user_loc_dim, user_dim, tweet_content_fact]
    table_names = ["user_date_dim", "tweet_date_dim", "location_dim", "user_dim", "tweet_details_fact"]
    credentials = {'username' : os.environ['db_username'],
                    'password' : os.environ['db_pass'],
                    'host' : os.environ['db_host'],
                    'port' : '5432',
                    'schema' : os.environ['db_schema']}
    
    
    queries = create_star_schema.queries_func()
    create_star_schema.create_tables(queries, credentials)
    
    for i in range(len(tables)):
        loader.data_load(tables[i], table_names[i], credentials)