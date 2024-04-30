from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os

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
    schedule_interval=None,  # No automatic scheduling
)

# Define the Python functions for each task (placeholders for your code)
def load_data_from_s3(bucket_name, object_key):
    # Create a session using default credentials
    s3 = boto3.client("s3")
    
    # Get the object from the S3 bucket
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    
    # Read the content of the object (assuming it's a CSV)
    csv_content = response["Body"].read()
    
    # Create a DataFrame from the CSV content
    df = pd.read_csv(BytesIO(csv_content))

    #Filtrar usuarios activos
    

    #Filtrar por la fecha de consulta
    

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
    task_id='data_load',
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









# Paso 1 - levantar data y filtrarla

#Levantar Data



#Filtrar usuarios Inactivos


#Filtrar la fecha relevante



# Paso 2.0 - TopCTR





# Paso 2.1 - TopProduct



# Paso 4 - DBWriting


