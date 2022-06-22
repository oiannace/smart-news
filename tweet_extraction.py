import json
import requests
import os
import datetime
import boto3

def lambda_handler(event, context):
    bucket_dest_name = 'pre-transformation-bucket'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_dest_name)

    
    key = os.environ['twitter_api_key']
    secret = os.environ['twitter_api_secret']
    bearer_token = os.environ['twitter_api_bearer_token']
    
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


    now = datetime.datetime.now()
    current_time_format = str(now.year)+"-"+str(now.month).zfill(2)+"-"+str(now.day-3).zfill(2)+"T"+str(now.hour).zfill(2)+":"+str(now.minute).zfill(2)+":"+str(now.second).zfill(2)+"Z"

    users_following_ids = [user['id'] for user in users_following.json()['data']]
    users_following_tweets = {}
    for user_id in users_following_ids:
        url_user_following_tweets = f'https://api.twitter.com/2/users/{user_id}/tweets?start_time={current_time_format}&exclude=retweets&tweet.fields=created_at'
        users_following_tweets[user_id]=requests.get(url=url_user_following_tweets, headers=headers)
    

    bucket.put_object(Key="user_ids.txt", Body=json.dumps(users_following_ids).encode("utf-8"))
    bucket.put_object(Key="user_full_data.txt", Body=json.dumps(users_following.json()).encode("utf-8"))
    for user_id in users_following_ids:
        bucket.put_object(Key=f"{user_id}_user_tweets.txt", Body=json.dumps(users_following_tweets[user_id].json()).encode("utf-8"))
    return {
        'statusCode': 200,
        'body': json.dumps('Tweet data successfully extracted and stored in s3 bucket')
    }