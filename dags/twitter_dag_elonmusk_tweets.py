from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from twitter_data_collection import run_twitter_etl_user_tweet
from twitter_data_transformation import process_twitter_data_elonmusktweet
from twitter_data_load_to_redshift import load_data_redshift

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 19),
    'retries': 1,
    'catchup':False,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'twitter_dag_elonmusk_tweets',
    default_args=default_args,
    description='twitter etl',
    schedule_interval=timedelta(days=1),
)

run_twitter_etl_user = PythonOperator(
    task_id='twitter_etl',
    python_callable=run_twitter_etl_user_tweet,
    op_kwargs={
        'execution_date': '{{ ds }}'
    },
    dag=dag, 
)

process_twitter_data = PythonOperator(
    task_id='process_twitter_data',
    python_callable=process_twitter_data_elonmusktweet,
    op_kwargs={
        'execution_date': '{{ ds }}'
    },
    dag=dag, 
)

load_twitter_data_redshift = PythonOperator(
    task_id='load_twitter_data_redshift',
    python_callable=load_data_redshift,
    op_kwargs={
        'execution_date': '{{ ds }}'
    },
    dag=dag, 
)

run_twitter_etl_user >> process_twitter_data >> load_twitter_data_redshift