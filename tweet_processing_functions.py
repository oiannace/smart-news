import nltk
import copy
import re
from nltk.corpus import wordnet
import pandas as pd

nltk.data.path.append("/tmp")
nltk.download("stopwords", download_dir='/tmp')
nltk.download('averaged_perceptron_tagger', download_dir='/tmp')
nltk.download('wordnet', download_dir='/tmp')
nltk.download('omw-1.4', download_dir='/tmp')
nltk.download('punkt', download_dir='/tmp')

#utility function to transform format of part of speech (pos) into usable format
def get_wordnet_pos(treebank_tag):

    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return ''

#function to tokenize words in a tweet, and stored into a dictionary by user
def word_tokenize(all_users_tweets, users_following_ids):
    user_tweets_word_tokenize = {}
    for user_id in users_following_ids:
        #iterating through each tweet for a particular user and tokenizing the words
        user_tweets_word_tokenize[user_id] = [nltk.word_tokenize(all_users_tweets[user_id][tweet]) for tweet in range(len(all_users_tweets[user_id]))]
    return user_tweets_word_tokenize

#function to remove a predefined list of words from the tweets that dont add value to analysis
def remove_stopwords(tweets_tokenized, users_following_ids):
    #set is faster to search
    stop_words = set(nltk.corpus.stopwords.words("english"))
    
    tweets_without_stopwords = {}
    for user_id in users_following_ids:
        tweets_without_stopwords[user_id] = copy.deepcopy(tweets_tokenized[user_id])
        for tweet_index in range(len(tweets_without_stopwords[user_id])):
            temp_tweet = tweets_without_stopwords[user_id][tweet_index]
            tweets_without_stopwords[user_id][tweet_index] = [word for word in temp_tweet if not word in stop_words]
    
    return tweets_without_stopwords

#function to keep only tokenized words that contain numbers and letters
def remove_punctuation(original_tweets, users_following_ids):
    user_tweets_no_punc = {}
    
    #regex pattern for only letters and numbers
    regex_pattern = r"[^a-zA-Z0-9\n]"
    
    for user_id in users_following_ids:
        user_tweets_no_punc[user_id] = [re.sub(regex_pattern, " ", original_tweets[user_id][tweet]) for tweet in range(len(original_tweets[user_id]))]
    return user_tweets_no_punc

#lemmatizing words: distilling word into its base form
def word_lemmatizer(user_tweets, pos_tags, users_following_ids):
    lemmatizer = nltk.stem.WordNetLemmatizer()
    lemmatized_tweets = {}
    
    for user_id in users_following_ids:
        lemmatized_tweets[user_id] = copy.deepcopy(user_tweets[user_id])
        for tweet_index in range(len(user_tweets[user_id])):
            lemmatized_tweets[user_id][tweet_index] = [lemmatizer.lemmatize(word,pos_tags[user_id][tweet_index][word]) if (pos_tags[user_id][tweet_index].get(word)) else word for word in user_tweets[user_id][tweet_index]]      
    return lemmatized_tweets

#creating a new data structure which contains pos tags for each word in a tweet for each user
def part_of_speech_tagging(user_tweets_tokenized, users_following_ids):
    pos_tags = {}
    
    for user_id in users_following_ids:
        pos_tags[user_id] = [nltk.pos_tag(user_tweets_tokenized[user_id][tweet_index]) for tweet_index in range(len(user_tweets_tokenized[user_id]))]
    return pos_tags


#function to convert odd list of pos tags to a dictionary for easy lookup for lem

#try to fins a more efficient way to do this
def pos_tags_data_structure_conv(pos_tags, users_following_ids):
    pos_tags_updated = {}
    
    for user_id in users_following_ids:
        pos_tags_updated[user_id] = copy.deepcopy(pos_tags[user_id])
        for tweet_index in range(len(pos_tags[user_id])):
            pos_tags_updated[user_id][tweet_index] =  {}
            for word in pos_tags[user_id][tweet_index]:
                #getting acceptable pos tags for lemmatize function
                lemm_acceptable_pos = get_wordnet_pos(word[1])
                if(lemm_acceptable_pos != ''):
                    pos_tags_updated[user_id][tweet_index][word[0]] = lemm_acceptable_pos
            
    return pos_tags_updated
    

def nested_dict_to_dataframe_user(user_data):
    nested_dict_columns = ['id', 'username', 'name', 'created_at', 'description', 'location']
    dataframe_cols = ['user_id', 'username', 'name', 'creation_date', 'bio', 'location']
    user_pd = pd.DataFrame(columns=dataframe_cols)
    
    for user in range(len(user_data['data'])):
        user_pd = user_pd.append({dataframe_cols[0] : user_data['data'][user][nested_dict_columns[0]],
                                       dataframe_cols[1] : user_data['data'][user][nested_dict_columns[1]],
                                       dataframe_cols[2] : user_data['data'][user][nested_dict_columns[2]],
                                       dataframe_cols[3] : user_data['data'][user][nested_dict_columns[3]],
                                       dataframe_cols[4] : user_data['data'][user][nested_dict_columns[4]],
                                       dataframe_cols[5] : user_data['data'][user][nested_dict_columns[5]]
                                      }, ignore_index=True)   
            
    return user_pd
 
def lemm_tweets_to_dataframe(lemm_tweets, tweet_ids, tweet_dates, users_following_ids):
    df_cols = ['user_id', 'tweet_id', 'tweet_date', 'tweet_content']
    tweets_df = pd.DataFrame(columns = df_cols)
    
    for user_id in users_following_ids:
        for tweet_index in range(len(lemm_tweets[user_id])):
            if(range(len(lemm_tweets[user_id])) != range(len(tweet_ids[user_id]))):
                print("Different num of tweets than ids")
            tweets_df = tweets_df.append({df_cols[0] : user_id,
                                      df_cols[1] : tweet_ids[user_id][tweet_index],
                                      df_cols[2] : tweet_dates[user_id][tweet_index],
                                      df_cols[3] : lemm_tweets[user_id][tweet_index]
                                      }, ignore_index=True)
            
    return tweets_df
        
    
    