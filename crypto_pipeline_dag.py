from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 22),
}

dag = DAG(
    'crypto_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for crypto data from CoinGecko to Snowflake using Glue and S3',
    schedule_interval='@daily',
    catchup=False
)

# Step 1: Fetch data from CoinGecko and upload to S3
def fetch_data():
    subprocess.run(["python3", "/opt/airflow/materials/fetch_and_upload.py"], check=True)

fetch_task = PythonOperator(
    task_id='fetch_crypto_data',
    python_callable=fetch_data,
    dag=dag,
)

# Step 2: Glue job to clean and transform JSON data -> parquet in S3
glue_clean_transform = GlueJobOperator(
    task_id='clean_transform_data',
    job_name='crypto-etl-job',
    iam_role_name='AWSGlueServiceRole-Crypto',
    script_location='s3://crypto-dataeng-project/scripts/etl_transform.py',
    region_name='us-east-1',
    aws_conn_id='aws_default',  # connection ID
    dag=dag
)


# Step 3: Glue job to load parquet into Snowflake tables
glue_load_snowflake = GlueJobOperator(
    task_id='load_into_snowflake',
    job_name='load_to_snowflake',
    iam_role_name='AWSGlueServiceRole-Crypto',
    script_location='s3://crypto-dataeng-project/scripts/load_to_snowflake.py',  # optional if already uploaded
    region_name='us-east-1',
    dag=dag
)

# Task dependency
fetch_task >> glue_clean_transform >> glue_load_snowflake
