from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql

# Parámetros del bucket y claves de acceso

ACCESS_KEY = 'AKIA47CRZ4EOG7FQAWPI'
SECRET_KEY = '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'

s3 = boto3.client(
 's3',
 region_name='us-east-1',
 aws_access_key_id=ACCESS_KEY,
 aws_secret_access_key=SECRET_KEY)

# Definimos los paths
s3_bucket="bucket-lmi"
ads_key= "DataSource/ads_views.csv"
product_key="DataSource/product_views.csv"
advertiser_ids_key="DataSource/advertiser_ids.csv"
output_ads_key="FilterData/ads_views_filtered.csv"
output_product_key="FilterData/product_views_filtered.csv"
output_top_20_product_key="ModelOutput/top_20_products.csv"
top_20_ctr_key="ModelOutput/top_20_ctr.csv"
pg_conn_str = ""

'''
# DAG con backfill
default_args = {
    'owner': 'airflow_user',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),  
}

dag = DAG(
    'Algo_de_Recomendacion',
    default_args=default_args,
    description='Levantar, procesar y escribir',
    schedule_interval='@daily', 
    start_date=datetime(2023, 4, 22), #corremos desde el 22 de abril (donde inicia la data generada)
    catchup=True,  # Enable catchup to run all missed tasks
)
'''


default_args = {
    'owner': 'airflow_user',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'Algo_de_Recomendacion',
    default_args=default_args,
    description='Levantar, procesar y escribir',
    schedule_interval='@daily',
    catchup = False,
)

def load_data_from_s3():
    
    # Get the objects from the S3 bucket
    ads_response = s3.get_object(Bucket=s3_bucket, Key=ads_key)
    product_response = s3.get_object(Bucket=s3_bucket, Key=product_key)
    advertiser_ids_response = s3.get_object(Bucket=s3_bucket, Key=advertiser_ids_key)

    # Read the content of the objects and create dataframes
    ads_views = pd.read_csv(BytesIO(ads_response["Body"].read()))
    product_views = pd.read_csv(BytesIO(product_response["Body"].read()))
    advertiser_ids = pd.read_csv(BytesIO(advertiser_ids_response["Body"].read()))

    # Filter 'ads_views' and 'product_views' by 'advertiser_id'
    ads_views_filtered = ads_views[ads_views['advertiser_id'].isin(advertiser_ids['advertiser_id'])]
    product_views_filtered = product_views[product_views['advertiser_id'].isin(advertiser_ids['advertiser_id'])]

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
    s3.put_object(Bucket=s3_bucket, Key=output_ads_key, Body=ads_csv)
    s3.put_object(Bucket=s3_bucket, Key=output_product_key, Body=product_csv)

    return output_ads_key, output_product_key
    

def top_ctr():
    
    # Retrieve the filtered ads data from S3
    response = s3.get_object(Bucket=s3_bucket, Key=output_ads_key)
    ads_data = pd.read_csv(BytesIO(response["Body"].read()))
    
    # Group by 'advertiser_id' and 'product_id', then calculate impressions and clicks
    impressions = ads_data[ads_data['type'] == 'impression'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='impressions')

    clicks = ads_data[ads_data['type'] == 'click'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='clicks')

    # Merge impressions and clicks data
    ctr_data = pd.merge(impressions, clicks, on=['advertiser_id', 'product_id'], how='left')

    # Handle missing 'clicks' or 'impressions' by filling NaN with 0
    ctr_data['clicks'].fillna(0, inplace=True)

    # Calculate CTR with safe handling for division by zero
    ctr_data['CTR'] = ctr_data.apply(lambda row: row['clicks'] / row['impressions'] if row['impressions'] > 0 else 0, axis=1)

    # Get the top 20 products by CTR for each advertiser
    top_20_ctr = (
        ctr_data
        .groupby('advertiser_id', group_keys=False)
        .apply(lambda x: x.nlargest(20, 'CTR'))  # Select top 20 by CTR for each advertiser
        [['advertiser_id', 'product_id', 'CTR']]  # Keep only necessary columns
    )

    # Save the output to S3 as CSV
    top_20_ctr_csv = top_20_ctr.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=s3_bucket, Key=top_20_ctr_key, Body=top_20_ctr_csv)

    return top_20_ctr_key

#TOP_Product

def top_product():
    
    # Retrieve the filtered product_views data from S3
    response = s3.get_object(Bucket=s3_bucket, Key=output_product_key)
    product_views = pd.read_csv(BytesIO(response["Body"].read()))
    
    # Count the number of views for each product by advertiser
    product_view_counts = product_views.groupby(
        ['advertiser_id', 'product_id']
    ).size().reset_index(name='views')  # Count the number of views

    # Get the top 20 products by views for each advertiser
    top_20_products = (
        product_view_counts
        .groupby('advertiser_id', group_keys=False)
        .apply(lambda x: x.nlargest(20, 'views'))  # Select the top 20 by views
        [['advertiser_id', 'product_id','views']]  # Keep only necessary columns
    )

    # Save the output to S3 as CSV
    top_20_csv = top_20_products.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=s3_bucket, Key=output_top_20_product_key, Body=top_20_csv)

    return output_top_20_product_key

def db_writing():
    
    # Load data from S3
    ctr_response = s3.get_object(Bucket=s3_bucket, Key=top_20_ctr_key)
    ctr_data = pd.read_csv(BytesIO(ctr_response["Body"].read()))
    
    product_views_response = s3.get_object(Bucket=s3_bucket, Key=output_top_20_product_key)
    product_views_data = pd.read_csv(BytesIO(product_views_response["Body"].read()))

    # Get yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).date()

    # Add 'Date' column to both dataframes
    ctr_data['date'] = str(yesterday)
    product_views_data['date'] = str(yesterday)

    # Connect to PostgreSQL database
    conn = psycopg2.connect(pg_conn_str)  # Establish a connection using a connection string
    cur = conn.cursor()  # Create a cursor object to execute SQL statements

    # Write CTR data to Table_CTR
    for _, row in ctr_data.iterrows():
        insert_query = sql.SQL("""
            INSERT INTO Top_CTR (advertiser_id, product_id, CTR, date)
            VALUES (%s, %s, %s, %s)
        """)
        cur.execute(insert_query, (row['advertiser_id'], row['product_id'], row['CTR'], row['date']))

    # Write Product Views data to Table_views
    for _, row in product_views_data.iterrows():
        insert_query = sql.SQL("""
            INSERT INTO Top_views (advertiser_id, product_id, views, date)
            VALUES (%s, %s, %s, %s)
        """)
        cur.execute(insert_query, (row['advertiser_id'], row['product_id'], row['views'], row['date']))

    # Commit the transaction to save the data
    conn.commit()

    # Close the cursor and the connection
    cur.close()
    conn.close()

# Definición de las tareas con PythonOperator
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

task_1 >> [task_2, task_3]  
[task_2, task_3] >> task_4 