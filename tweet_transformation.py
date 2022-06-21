import json
import lib.tweet_processing_functions as tweet_processing_functions
import boto3
from io import StringIO
import random
import pandas as pd

def lambda_handler(event, context):

    bucket_source_name = 'pre-transformation-bucket'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_source_name)

    response = bucket.download_file('user_ids.txt', "/tmp/user_ids.txt")
    users_following_ids = json.loads(open("/tmp/user_ids.txt", 'r').read())
    
    response = bucket.download_file('user_full_data.txt', "/tmp/user_full_data.txt")
    users_following = json.loads(open("/tmp/user_full_data.txt", 'r').read())
    #print(type(users_following), user_df)
    
    
    
    users_following_tweets = {}
    for user_id in users_following_ids:
        response = bucket.download_file(f"{user_id}_user_tweets.txt", f"/tmp/{user_id}_user_tweets.txt")
        users_following_tweets[user_id] = json.loads(open(f'/tmp/{user_id}_user_tweets.txt', 'r').read())
        
    user_tweets_simple = {}
    user_tweets_ids = {}
    user_tweets_dates = {}
    for user_id in users_following_ids:
        user_tweets_simple[user_id] = [users_following_tweets[user_id]['data'][i]['text'] for i in range(users_following_tweets[user_id]['meta']['result_count'])]
        user_tweets_ids[user_id] =  [users_following_tweets[user_id]['data'][i]['id'] for i in range(users_following_tweets[user_id]['meta']['result_count'])]
        user_tweets_dates[user_id] =  [users_following_tweets[user_id]['data'][i]['created_at'] for i in range(users_following_tweets[user_id]['meta']['result_count'])]
    
    #tokenize words in tweet for pos purpose
    user_tweets_word_tokenize_for_pos = tweet_processing_functions.word_tokenize(user_tweets_simple, users_following_ids)
    
    #tag each word with the part of speech in order to use for lemmatization in later stage
    pos_tags = tweet_processing_functions.part_of_speech_tagging(user_tweets_word_tokenize_for_pos, users_following_ids)
    
    pos_tags_updated = tweet_processing_functions.pos_tags_data_structure_conv(pos_tags, users_following_ids)
   
    #remove punctuation
    user_tweets_no_punc = tweet_processing_functions.remove_punctuation(user_tweets_simple, users_following_ids)
    
    #tokenization for regular purposes after punctuation is removed
    user_tweets_word_tokenize_reg= tweet_processing_functions.word_tokenize(user_tweets_no_punc, users_following_ids)
    
    #filter tweets for words that dont add valuable information, nltk refers to this list as stopwords
    user_tweets_without_stopwords = tweet_processing_functions.remove_stopwords(user_tweets_word_tokenize_reg, users_following_ids)
    
    #Stemming words - basically chopping off the ends of words to get the base form
    user_tweets_lemmatized = tweet_processing_functions.word_lemmatizer(user_tweets_without_stopwords, pos_tags_updated, users_following_ids)
    
    tweets_lemm_df = tweet_processing_functions.lemm_tweets_to_dataframe(user_tweets_lemmatized, user_tweets_ids, user_tweets_dates, users_following_ids)
    user_df = tweet_processing_functions.nested_dict_to_dataframe_user(users_following)
    
    
    i = 0
    surrogate_key_list = random.sample(range(10000, 99999), 1000)



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
    
    
    tables = [complete_table, user_dim, user_loc_dim, tweet_date_dim, user_date_dim]
    table_names = ['complete_table', 'user_dim', 'user_loc_dim', 'tweet_date_dim', 'user_date_dim']
    
    for table_index in range(len(tables)):
        csv_buffer = StringIO()
        tables[table_index].to_csv(csv_buffer)
        bucket.put_object(Key = f'{table_names[table_index]}.csv', Body=csv_buffer.getvalue())
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Tweets successfully transformed and stored in s3 bucket')
    }

