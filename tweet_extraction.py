import requests
import os

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

url_my_followers = f'https://api.twitter.com/2/users/{my_id}/following'
my_followers = requests.get(url=url_my_followers, headers=headers)
print(my_followers.status_code)
print(my_followers.json())