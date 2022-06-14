#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2

init_q = '''DROP TABLE IF EXISTS processed_tweets.tweet_details_fact,
                                processed_tweets.location_dim,
                                processed_tweets.user_dim,
                                processed_tweets.tweet_date_dim,
                                processed_tweets.user_date_dim;
            DROP SCHEMA IF EXISTS processed_tweets;
            CREATE SCHEMA IF NOT EXISTS processed_tweets
            AUTHORIZATION postgres;'''

tweet_create_date_dim_q = '''
    CREATE  TABLE processed_tweets.tweet_date_dim ( 
        date_key             integer Primary key,
        year                 integer,
        month                integer,
        day                  integer,
        hour                 integer,
        minute               integer
);
'''

user_create_date_dim_q = '''
    CREATE  TABLE processed_tweets.user_date_dim ( 
        date_key             integer Primary key,
        year                 integer,
        month                integer,
        day                  integer,
        hour                 integer,
        minute               integer
);
'''

create_user_dim_q = '''
    CREATE  TABLE processed_tweets.user_dim ( 
        user_key            integer Primary key ,
        location_key        integer,
        creation_date_key    integer,
        user_id              bigint,
        username                varchar(100)   ,
        name                 varchar(100),
        bio          varchar(200),
        
        FOREIGN KEY (creation_date_key) REFERENCES processed_tweets.user_date_dim(date_key),
        FOREIGN KEY (location_key) REFERENCES processed_tweets.location_dim(location_key)
);
'''

create_location_dim_q = '''
    CREATE  TABLE processed_tweets.location_dim ( 
	   location_key           integer Primary key ,
	   location                varchar(100)
);
'''


create_fact_tbl_q = '''
    CREATE  TABLE processed_tweets.tweet_details_fact ( 
	   user_date_key             integer  ,
       tweet_date_key            integer , 
	   location_key         integer   ,
	   user_key            integer   ,
	   tweet_id           bigint  ,
	   tweet_content       varchar(500),
	   
	   PRIMARY KEY (tweet_id),
	   FOREIGN KEY ( user_date_key ) REFERENCES processed_tweets.user_date_dim( date_key )   ,
       FOREIGN KEY ( tweet_date_key ) REFERENCES processed_tweets.tweet_date_dim( date_key ),
	   FOREIGN KEY ( location_key ) REFERENCES processed_tweets.location_dim( location_key )   ,
	   FOREIGN KEY ( user_key ) REFERENCES processed_tweets.user_dim( user_key ) 
 );
'''

queries = [init_q, user_create_date_dim_q, tweet_create_date_dim_q, create_location_dim_q,  create_user_dim_q, create_fact_tbl_q]

#username and password are specific to your postgreSQL account
#set up environment variables
db_name = "db_name"
username = "postgres"
password = "password"
host = "db-instance.random_letters.ca-central-1.rds.amazonaws.com" #will be in this form
port = "5432"
conn = psycopg2.connect(host = host,
                        dbname = db_name,
                        port = port,
                        user = username,
                        password = password)

cur = conn.cursor()

for query in queries:
    cur.execute(query)
    conn.commit()

if(conn):
    conn.close()
    cur.close()