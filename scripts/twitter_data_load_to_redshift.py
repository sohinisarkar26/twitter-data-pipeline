import boto3
import psycopg2

import pandas as pd
from io import BytesIO
from datetime import datetime
import json

def load_data_redshift(execution_date):

    processed_data_path = f"s3://run-twitter-etl-user-tweet/processed/airflow_execution_date={execution_date}/elonmusk_tweets_processed.csv"

    # Redshift Serverless Configuration
    workgroup_name = 'twitter-etl'

    # COPY command
    copy_command = f"""
    COPY twitter_feed_elonmusk
    FROM '{processed_data_path}'
    IAM_ROLE 'arn:aws:iam::533267159644:role/ec2_s3_twitter_etl'
    CSV
    IGNOREHEADER 1;
    """

    # Connection details
    conn_details = {
        'dbname': 'dev',
        'user': 'etl',
        'password': 'password',
        'host': 'twitter-etl.533267159644.us-east-1.redshift-serverless.amazonaws.com',
        'port': 5439
    }
        
    # Execute the COPY command
    try:
        conn = psycopg2.connect(**conn_details)
        cur = conn.cursor()
        cur.execute(copy_command)
        conn.commit()  # Commit the transaction
        print("Data loaded successfully")
    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

