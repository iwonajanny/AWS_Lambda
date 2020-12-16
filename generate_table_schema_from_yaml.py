import json
import boto3
import awswrangler as wr
import yaml
import os

s3 = boto3.client('s3')

def get_yaml_from_s3(filename, bucket):
    file_obj = s3.get_object(Bucket = bucket, Key = filename)
    yaml_file = yaml.safe_load(file_obj['Body'])
    return yaml_file
    
def lambda_handler(event, context):
    key = os.environ['key']
    bucket = os.environ['bucket'] 
    yaml_file = get_yaml_from_s3(key,bucket)
    table = yaml_file.get('name')
    db = yaml_file.get('database')
    columns = yaml_file.get('columns')
    col_types = {}
    for col in columns:
        col_types[col['name']]=col['type']
    
    database = db
    wr.catalog.create_parquet_table(
        database = database,
        table = table,
        path = 's3://{}/{}/{}'.format(bucket,db,table),
        columns_types = col_types
    )