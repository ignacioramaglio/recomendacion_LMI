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
end_date_today = datetime.combine(datetime.today().date(), datetime.min.time())


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
    start_date=datetime(2024, 5, 2), #corremos desde el 2 de mayo (data inicia el 1 de mayo)
   # end_date = end_date_today,
    catchup=True,  # Ponemos el catch up.
)

def load_data_from_s3(**context):
    
    # Levantamos los objetos del bucket del S3
    ads_response = s3.get_object(Bucket=s3_bucket, Key=ads_key)
    product_response = s3.get_object(Bucket=s3_bucket, Key=product_key)
    advertiser_ids_response = s3.get_object(Bucket=s3_bucket, Key=advertiser_ids_key)

    # Los convertimos a dataframe
    ads_views = pd.read_csv(BytesIO(ads_response["Body"].read()))
    product_views = pd.read_csv(BytesIO(product_response["Body"].read()))
    advertiser_ids = pd.read_csv(BytesIO(advertiser_ids_response["Body"].read()))

    # Filtramos 'ads_views' y 'product_views' por 'advertiser_id'
    ads_views_filtered = ads_views[ads_views['advertiser_id'].isin(advertiser_ids['advertiser_id'])]
    product_views_filtered = product_views[product_views['advertiser_id'].isin(advertiser_ids['advertiser_id'])]

    # Dia anterior a la fecha de ejecución
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).date()

    # Filtramos por esa fecha
    ads_views_filtered = ads_views_filtered[ads_views_filtered['date'] == str(yesterday)]
    product_views_filtered = product_views_filtered[product_views_filtered['date'] == str(yesterday)]

    # Convertimos los dataframes en csv
    ads_csv = ads_views_filtered.to_csv(index=False).encode("utf-8")
    product_csv = product_views_filtered.to_csv(index=False).encode("utf-8")

    # Guardamos un csv por cada fecha de ejecucion
    dated_output_ads_key = f"{output_ads_key[:-4]}_{yesterday}.csv"
    dated_output_product_key = f"{output_product_key[:-4]}_{yesterday}.csv"

    # Guardamos los csv en S3
    s3.put_object(Bucket=s3_bucket, Key=dated_output_ads_key, Body=ads_csv)
    s3.put_object(Bucket=s3_bucket, Key=dated_output_product_key, Body=product_csv)

    return dated_output_ads_key, dated_output_product_key



def top_ctr(**context):
    
    # Dia anterior a la fecha de ejecución
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).date()

    dated_output_ads_key = f"{output_ads_key[:-4]}_{yesterday}.csv"

    # Levantamos los objetos del bucket del S3
    response = s3.get_object(Bucket=s3_bucket, Key=dated_output_ads_key)
    ads_data = pd.read_csv(BytesIO(response["Body"].read()))
    
    # Agrupamos por 'advertiser_id' y 'product_id', y calculamos impressions y clicks
    impressions = ads_data[ads_data['type'] == 'impression'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='impressions')

    clicks = ads_data[ads_data['type'] == 'click'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='clicks')

   
    ctr_data = pd.merge(impressions, clicks, on=['advertiser_id', 'product_id'], how='left')
    ctr_data['clicks'].fillna(0, inplace=True)

    # Calculamos CTR considerando los casos donde impressions pueden ser cero
    ctr_data['CTR'] = ctr_data.apply(lambda row: row['clicks'] / row['impressions'] if row['impressions'] > 0 else 0, axis=1)

    # Obtenemos el top 20 de productos por CTR para cada advertiser
    top_20_ctr = (
        ctr_data
        .groupby('advertiser_id', group_keys=False)
        .apply(lambda x: x.nlargest(20, 'CTR'))  
        [['advertiser_id', 'product_id', 'CTR']]  
    )

    
    dated_top_20_ctr_key = f"{top_20_ctr_key[:-4]}_{yesterday}.csv"

    top_20_ctr_csv = top_20_ctr.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=s3_bucket, Key=dated_top_20_ctr_key, Body=top_20_ctr_csv)

    return dated_top_20_ctr_key


#TOP_Product

def top_product(**context):
    
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).date()

    dated_output_product_key = f"{output_product_key[:-4]}_{yesterday}.csv"

    response = s3.get_object(Bucket=s3_bucket, Key=dated_output_product_key)
    product_views = pd.read_csv(BytesIO(response["Body"].read()))
    
    # Contamos el numero de views por cada producto para cada advertiser
    product_view_counts = product_views.groupby(
        ['advertiser_id', 'product_id']
    ).size().reset_index(name='views')  

    #Obtenemos el top 20 de productos por views para cada advertiser
    top_20_products = (
        product_view_counts
        .groupby('advertiser_id', group_keys=False)
        .apply(lambda x: x.nlargest(20, 'views'))  
        [['advertiser_id', 'product_id','views']]  
    )

  
    dated_output_top_20_product_key = f"{output_top_20_product_key[:-4]}_{yesterday}.csv"

    top_20_csv = top_20_products.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=s3_bucket, Key=dated_output_top_20_product_key, Body=top_20_csv)

    return dated_output_top_20_product_key


def db_writing(**context):
    
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).date()

    dated_top_20_ctr_key = f"{top_20_ctr_key[:-4]}_{yesterday}.csv"
    dated_output_top_20_product_key = f"{output_top_20_product_key[:-4]}_{yesterday}.csv"

    ctr_response = s3.get_object(Bucket=s3_bucket, Key=dated_top_20_ctr_key)
    ctr_data = pd.read_csv(BytesIO(ctr_response["Body"].read()))
    
    product_views_response = s3.get_object(Bucket=s3_bucket, Key=dated_output_top_20_product_key)
    product_views_data = pd.read_csv(BytesIO(product_views_response["Body"].read()))

    ctr_data['date'] = str(yesterday)
    product_views_data['date'] = str(yesterday)

    # Conexión a la base de datos PostgreSQL
    conn = psycopg2.connect(
        database = "postgres",
        user = "user_lmi",
        password = "basededatoslmi",
        host = "db-tp-lmi.cjuseewm8uut.us-east-1.rds.amazonaws.com",
        port = "5432" )
    cur = conn.cursor()  

    # Insertamos los valores en la tabla Top_CTR
    for _, row in ctr_data.iterrows():
        insert_query = sql.SQL("""
            INSERT INTO Top_CTR (advertiser_id, product_id, CTR, date)
            VALUES (%s, %s, %s, %s)
        """)
        cur.execute(insert_query, (row['advertiser_id'], row['product_id'], row['CTR'], row['date']))

    # Insertamos los valores en la tabla Top_views
    for _, row in product_views_data.iterrows():
        insert_query = sql.SQL("""
            INSERT INTO Top_views (advertiser_id, product_id, views, date)
            VALUES (%s, %s, %s, %s)
        """)
        cur.execute(insert_query, (row['advertiser_id'], row['product_id'], row['views'], row['date']))

    conn.commit()

    cur.close()
    conn.close()

# Definición de las tareas con PythonOperator, habilitamos el contexto para usar la fecha de ejecución.
task_1 = PythonOperator(
    task_id='data_load_and_filtering',
    python_callable=load_data_from_s3,
    provide_context = True,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='TopCTR',
    python_callable=top_ctr,
    provide_context = True,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='TopProduct',
    python_callable=top_product,
    provide_context = True,
    dag=dag,
)

task_4 = PythonOperator(
    task_id='DBWriting',
    python_callable=db_writing,
    provide_context = True,
    dag=dag,
)

# Seteamos dependencias

task_1 >> [task_2, task_3]  
[task_2, task_3] >> task_4 
