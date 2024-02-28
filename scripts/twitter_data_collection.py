import tweepy
import pandas as pd
import json
from datetime import datetime
#s3fs is a PyFilesystem interface to Amazon S3 cloud storage
import s3fs
from collections import Counter
import requests

#get keys from twitter dev account
access_key = access_key
access_secret = access_secret
consumer_key = consumer_key
consumer_secret = consumer_secret
BEARER_TOKEN = BEARER_TOKEN

def run_twitter_etl_user_tweet(execution_date):
    # Twitter authentication
    client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

    # pulling data from twitter for tweets by elonmusk
    user_id = client.get_user(username='elonmusk').data.id

    max_results = 100
    counter = 0
    next_token = None

    tweet_fields = ['author_id','text','entities','public_metrics','created_at']
    tweet_list = []
    partition_path = f"s3://run-twitter-etl-user-tweet/unprocessed/airflow_execution_date={execution_date}/elonmusk_tweets.csv"

    #using pagination as max_results allowed is 100 and we want to pull 300 tweets

    while counter <15 :
        tweets = client.get_users_tweets(id=user_id, 
                                        max_results=max_results,
                                        pagination_token=next_token,
                                        exclude='replies',
                                        tweet_fields=tweet_fields)
        next_token = tweets.meta.get('next_token', None)
        for tweet in tweets.data:
            refined_tweet = {'author': tweet.author_id,
                            'text' : tweet.text,
                            'entities' : tweet.entities,
                            'public_metrics' : tweet.public_metrics,
                            'retweet_count' : tweet.public_metrics['retweet_count'],
                            'like_count' : tweet.public_metrics['like_count'],
                            'impression_count' : tweet.public_metrics['impression_count'],
                            'created_at' : tweet.created_at}              
            
            tweet_list.append(refined_tweet)
        counter = counter+1

    #print(tweet_list[50])
    #store this data as csv
    df = pd.DataFrame(tweet_list)
    df.to_csv(partition_path)
