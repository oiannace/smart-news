import nltk
import copy

#run the following line once, then it can be commented out
#nltk.download("stopwords")

def word_tokenize(all_users_tweets, users_following_ids):
    user_tweets_word_tokenize = {}
    for user_id in users_following_ids:
        user_tweets_word_tokenize[user_id] = [nltk.word_tokenize(all_users_tweets[user_id][tweet]) for tweet in range(len(all_users_tweets[user_id]))]
    return user_tweets_word_tokenize

def remove_stopwords(tweets_tokenized, users_following_ids):
    #set is faster to search
    stop_words = set(nltk.corpus.stopwords.words("english"))
    
    tweets_without_stopwords = {}
    for user_id in users_following_ids:
        tweets_without_stopwords[user_id] = tweets_tokenized[user_id]
        for tweet_index in range(len(tweets_without_stopwords[user_id])):
            temp_tweet = copy.deepcopy(tweets_without_stopwords[user_id][tweet_index])
            tweets_without_stopwords[user_id][tweet_index] = [word for word in temp_tweet if not word in stop_words]
    
    return tweets_without_stopwords