import json
import lib.tweet_processing_functions as tweet_processing_functions
import boto3
from io import StringIO

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
    
    tweets_lemmatized_df = tweet_processing_functions.lemm_tweets_to_dataframe(user_tweets_lemmatized, user_tweets_ids, user_tweets_dates, users_following_ids)
    users_df = tweet_processing_functions.nested_dict_to_dataframe_user(users_following)
    
    
    csv_buffer_tweets = StringIO()
    csv_buffer_users = StringIO()
    tweets_lemmatized_df.to_csv(csv_buffer_tweets)
    users_df.to_csv(csv_buffer_users)
    
    bucket.put_object(Key = 'tweets_lemmatized_df.csv', Body=csv_buffer_tweets.getvalue())
    bucket.put_object(Key = 'users_df.csv', Body=csv_buffer_users.getvalue())
    
    return {
        'statusCode': 200,
        'body': json.dumps('Tweets successfully transformed and stored in post transformation s3 bucket')
    }