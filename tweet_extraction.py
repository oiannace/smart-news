import requests
import os
import datetime
import json
import nltk
import tweet_processing_functions

#need to run the following line once, then it can be commented out
#nltk.download('punkt')

key = os.environ.get('TWITTER_API_KEY')
secret = os.environ.get('TWITTER_API_SECRET')
bearer_token = os.environ.get('TWITTER_BEARER_TOKEN')

twitter_users = tuple(['KevinKenson'])

my_username = 'ornelloiannace'

url_me = f'https://api.twitter.com/2/users/by/username/{my_username}'

headers = {
    "Authorization": "Bearer " + bearer_token
}
my_user = requests.get(url=url_me, headers=headers)
print(my_user.status_code)
my_id = my_user.json()['data']['id']

url_users_following = f'https://api.twitter.com/2/users/{my_id}/following?user.fields=created_at,location,description'
users_following = requests.get(url=url_users_following, headers=headers)
print(users_following.status_code)

user_df = tweet_processing_functions.nested_dict_to_dataframe_user(users_following.json())
print(user_df)


now = datetime.datetime.now()
current_time_format = str(now.year)+"-"+str(now.month).zfill(2)+"-"+str(now.day-3).zfill(2)+"T"+str(now.hour).zfill(2)+":"+str(now.minute).zfill(2)+":"+str(now.second).zfill(2)+"Z"
print(current_time_format)
exclude = ['retweets', 'replies']

users_following_ids = [user['id'] for user in users_following.json()['data']]
users_following_tweets = {}
for user_id in users_following_ids:
    url_user_following_tweets = f'https://api.twitter.com/2/users/{user_id}/tweets?start_time={current_time_format}&exclude=retweets'
    users_following_tweets[user_id]=requests.get(url=url_user_following_tweets, headers=headers)
    
user_tweets_simple = {}
user_tweets_ids = {}
for user_id in users_following_ids:
    user_tweets_simple[user_id] = [users_following_tweets[user_id].json()['data'][i]['text'] for i in range(users_following_tweets[user_id].json()['meta']['result_count'])]
    user_tweets_ids[user_id] =  [users_following_tweets[user_id].json()['data'][i]['id'] for i in range(users_following_tweets[user_id].json()['meta']['result_count'])]

#tokenize words in tweet for pos purpose
user_tweets_word_tokenize_for_pos = tweet_processing_functions.word_tokenize(user_tweets_simple, users_following_ids)

#tag each word with the part of speech in order to use for lemmatization in later stage
pos_tags = tweet_processing_functions.part_of_speech_tagging(user_tweets_word_tokenize_for_pos, users_following_ids)
#print(pos_tags[user_id][1])
pos_tags_updated = tweet_processing_functions.pos_tags_data_structure_conv(pos_tags, users_following_ids)
#print(pos_tags_updated[user_id][1])
#print(pos_tags_updated[user_id][1]['needs'])
#remove punctuation
user_tweets_no_punc = tweet_processing_functions.remove_punctuation(user_tweets_simple, users_following_ids)

#tokenization for regular purposes after punctuation is removed
user_tweets_word_tokenize_reg= tweet_processing_functions.word_tokenize(user_tweets_no_punc, users_following_ids)

#filter tweets for words that dont add valuable information, nltk refers to this list as stopwords
user_tweets_without_stopwords = tweet_processing_functions.remove_stopwords(user_tweets_word_tokenize_reg, users_following_ids)

#print(user_tweets_without_stopwords[user_id][1])

#Stemming words - basically chopping off the ends of words to get the base form
user_tweets_lemmatized = tweet_processing_functions.word_lemmatizer(user_tweets_without_stopwords, pos_tags_updated, users_following_ids)
#print(user_tweets_lemmatized)

tweets_lemmatized_df = tweet_processing_functions.lemm_tweets_to_dataframe(user_tweets_lemmatized, user_tweets_ids, users_following_ids)
print(tweets_lemmatized_df)
def load_data():
    return tweets_lemmatized_df, user_df
