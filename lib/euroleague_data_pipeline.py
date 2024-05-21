from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery

# Constants for file paths and BigQuery identifiers
SHOT_DATA_FILE = '/Users/chloetoledano/dataeuro/shot_data.csv'
PROCESSED_DATA_FILE = '/Users/chloetoledano/dataeuro/processed_data.csv'
PROJECT_ID = 'keen-genre-365508'
DATASET_ID = 'euroleague_data'
TABLE_ID = 'shot_data'

def fetch_data():
    from euroleague_api.shot_data import ShotData
    try:
        # Initialize with the competition code if necessary
        shot_data = ShotData(competition='E')
        df = shot_data.get_game_shot_data(season=2022, gamecode=1)
        
        # Debug: Print the data fetched
        print("Data fetched from API:")
        print(df.head())
        
        if df.empty:
            print("Warning: No data fetched.")
        else:
            df.to_csv(SHOT_DATA_FILE, index=False)
            print(f"Data saved to {SHOT_DATA_FILE}")
    except Exception as e:
        print(f"Error occurred: {e}")

def process_data():
    try:
        df = pd.read_csv(SHOT_DATA_FILE)
        df['date'] = pd.to_datetime(df['date'])
        df.to_csv(PROCESSED_DATA_FILE, index=False)
    except Exception as e:
        print(f"Error occurred during processing data: {e}")

def load_data_to_bigquery():
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Automatically infer the schema
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append to the table
        )
        with open(PROCESSED_DATA_FILE, "rb") as file:
            job = client.load_table_from_file(file, table_ref, job_config=job_config)
            job.result()
        print(f"Loaded data into {TABLE_ID} in BigQuery")
    except Exception as e:
        print(f"Error occurred during loading data to BigQuery: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

with DAG('euroleague_data_pipeline', default_args=default_args, schedule='@daily') as dag:
    fetch_task = PythonOperator(task_id='fetch_data', python_callable=fetch_data)
    process_task = PythonOperator(task_id='process_data', python_callable=process_data)
    load_bq_task = PythonOperator(task_id='load_to_bigquery', python_callable=load_data_to_bigquery)

    fetch_task >> process_task >> load_bq_task

fetch_data()
process_data()
load_data_to_bigquery()