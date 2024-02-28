import boto3

import pandas as pd
import ast
from io import BytesIO
from datetime import datetime
import json

bucket_name = 'run-twitter-etl-user-tweet'
folder_path_unprocessed = 'unprocessed/'

tweets_combined_df = pd.DataFrame(columns=["Text","entities","normalized_text","mentions","retweet_count","like_count","impression_count","created_at"])

# Function to safely convert string to dict and extract 'normalized_text'
def extract_normalized_text(row):
    try:
        # Convert string to dictionary
        dict_row = ast.literal_eval(row)
        # Navigate through the dictionary to extract 'normalized_text'
        annotations = dict_row.get('annotations', {})
        normalized_texts = [annotation['normalized_text'] for annotation in annotations]
        return normalized_texts
    except (ValueError, SyntaxError):
        # In case of conversion error or missing keys
        return None

def extract_mentions(row):
    try:
        # Convert string to dictionary
        dict_row = ast.literal_eval(row)
        # Navigate through the dictionary to extract 'normalized_text'
        mentions = dict_row.get('mentions', {})
        username = [mention['username'] for mention in mentions]
        return username
    except (ValueError, SyntaxError):
        # In case of conversion error or missing keys
        return None

def process_twitter_data_elonmusktweet(execution_date):

    s3 = boto3.client('s3',region_name='us-east-1')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path_unprocessed)
    if 'Contents' in response:
        for obj in response['Contents']:
            # Make sure to skip directories/folders
            if obj['Key'].endswith('/'):
                continue
            
            obj_data = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
            data = obj_data['Body'].read()
            df = pd.read_csv(BytesIO(data))
            df['normalized_text'] = df['entities'].apply(extract_normalized_text)
            df['mentions'] = df['entities'].apply(extract_mentions)
            df = df[['text','entities','normalized_text','mentions','retweet_count','like_count','impression_count','created_at']]
    else:
        print("No objects found.")
    
    # Concatenate all DataFrames into one DataFrame
    tweets_combined_df[['Text','entities','normalized_text','mentions','retweet_count','like_count','impression_count','created_at']] = pd.concat([df])
    #tweets_combined_df.to_csv('processed_tweets_elonmusk.csv', index=False)

    # write processed data to s3
    processed_data_path = f"s3://run-twitter-etl-user-tweet/processed/airflow_execution_date={execution_date}/elonmusk_tweets_processed.csv"
    tweets_combined_df.to_csv(processed_data_path, index=False)
