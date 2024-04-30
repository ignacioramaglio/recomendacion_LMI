from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime, timedelta
import psycopg2

# Set parameters for the S3 bucket and object keys
s3_bucket = "bucket-name"
ads_key = "path/to/ads_views.csv"
product_key = "path/to/product_views.csv"
advertiser_ids_key = "path/to/advertiser_ids.csv"
output_ads_key = "output/path/filtered_ads_views.csv"
output_product_key = "output/path/filtered_product_views.csv"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow_user',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'Algo de Recomendacion',
    default_args=default_args,
    description='Levantar, procesar y escribir',
    schedule_interval='@daily',
    catchup = False,
)

def load_data_from_s3(bucket_name, ads_key, product_key, advertiser_ids_key):
    # Create a session using default credentials
    s3 = boto3.client("s3")
    
    # Get the objects from the S3 bucket
    ads_response = s3.get_object(Bucket=bucket_name, Key=ads_key)
    product_response = s3.get_object(Bucket=bucket_name, Key=product_key)
    advertiser_ids_response = s3.get_object(Bucket=bucket_name, Key=advertiser_ids_key)

    # Read the content of the objects and create dataframes
    ads_views = pd.read_csv(BytesIO(ads_response["Body"].read()))
    product_views = pd.read_csv(BytesIO(product_response["Body"].read()))
    advertiser_ids = pd.read_csv(BytesIO(advertiser_ids_response["Body"].read()))

    # Filter 'ads_views' and 'product_views' by 'advertiser_id'
    ads_views_filtered = ads_views[ads_views['advertiser_id'].isin(advertiser_ids)]
    product_views_filtered = product_views[product_views['advertiser_id'].isin(advertiser_ids)]

    # Get yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).date()

    # Filter by 'date' to get only yesterday's data
    ads_views_filtered = ads_views_filtered[ads_views_filtered['date'] == str(yesterday)]
    product_views_filtered = product_views_filtered[product_views_filtered['date'] == str(yesterday)]

    # Save the filtered dataframes to S3 as CSV
    # Write the CSV data to BytesIO
    ads_csv = ads_views_filtered.to_csv(index=False).encode("utf-8")
    product_csv = product_views_filtered.to_csv(index=False).encode("utf-8")

    # Put the CSV data to S3
    s3.put_object(Bucket=bucket_name, Key=output_ads_key, Body=ads_csv)
    s3.put_object(Bucket=bucket_name, Key=output_product_key, Body=product_csv)

    return output_ads_key, output_product_key
    

def top_ctr():
    # Your code for TopCTR
    pass

def top_product():
    # Your code for TopProduct
    pass

def db_writing():
    # Your code to write to the database
    pass

# Define the tasks with PythonOperator
task_1 = PythonOperator(
    task_id='data_load_and_filtering',
    python_callable=load_data_from_s3,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='TopCTR',
    python_callable=top_ctr,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='TopProduct',
    python_callable=top_product,
    dag=dag,
)

task_4 = PythonOperator(
    task_id='DBWriting',
    python_callable=db_writing,
    dag=dag,
)

# Dependencias

task_1 >> [task_2, task_3]  # Task 1 is a predecessor for Tasks 2 and 3
[task_2, task_3] >> task_4  # Task 4 depends on both Tasks 2 and 3