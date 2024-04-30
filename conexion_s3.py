import boto3
import pandas as pd
ACCESS_KEY = 'AKIA47CRZ4EOG7FQAWPI'
SECRET_KEY = '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'
s3 = boto3.client(
 's3',
 region_name='us-east-1',
 aws_access_key_id=ACCESS_KEY,
 aws_secret_access_key=SECRET_KEY)
bucket_name = 'bucket-lmi'
file_name1 = 'DataSource/ads_views.csv'
file_name2 = 'DataSource/advertiser_ids.csv'
file_name3 = 'DataSource/product_views.csv'

file_object = s3.get_object(Bucket=bucket_name, Key=file_name1)
df_ads_views = pd.read_csv(file_object['Body'])

file_object = s3.get_object(Bucket=bucket_name, Key=file_name2)
df_advertiser_ids = pd.read_csv(file_object['Body'])

file_object = s3.get_object(Bucket=bucket_name, Key=file_name3)
df_product_views = pd.read_csv(file_object['Body'])

print(df_advertiser_ids)