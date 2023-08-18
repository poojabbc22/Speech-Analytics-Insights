import csv
import json
from datetime import datetime, timedelta
from pandas import json_normalize
from collections import defaultdict
import boto3
import io
import sys
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json


def read_json(s3_path):
    s3 = boto3.client('s3')
    bucket = s3_path.split('/')[2]
    key = '/'.join(s3_path.split('/')[3:])
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    return data

def flatten_json(data, parent_key='', sep='.'):
    flattened = {}

    def flatten(element, prefix=""):
        nonlocal flattened
        if isinstance(element, dict):
            for key, value in element.items():
                new_key = f"{prefix}{sep}{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    flatten(value, new_key)
                else:
                    flattened[new_key] = value
        elif isinstance(element, list):
            for index, value in enumerate(element):
                new_key = f"{prefix}{sep}{index}" if prefix else str(index)
                if isinstance(value, (dict, list)):
                    flatten(value, new_key)
                else:
                    flattened[new_key] = value

    flatten(data, parent_key)
    return flattened

def convert_to_csv(data_list, s3_path,pola_path):
    all_records = []
    for data in data_list:
        flattened_data = flatten_json(data)
        all_records.append(flattened_data)

    df = pd.json_normalize(all_records)
    df.to_csv(s3_path,index=False)
    df.to_csv(pola_path,index=False)

def convert_to_parquet(data_list, s3_path,pola_path):
    all_records = []
    for data in data_list:
        flattened_data = flatten_json(data)
        all_records.append(flattened_data)
    df = json_normalize(all_records)

    table = pa.Table.from_pandas(df)
    
    pq_buffer = io.BytesIO()  
    pq.write_table(table, pq_buffer)
    pq_buffer.seek(0)
    
    s3_client = boto3.client('s3')
    bucket_name, s3_file_key = s3_path.split('/', 3)[2:]
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=pq_buffer.getvalue())
    
    bucket_name, s3_file_key = pola_path.split('/', 3)[2:]
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_key, Body=pq_buffer.getvalue())
	

if __name__ == "__main__":

    current_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    print(current_date)
    apis = ["components", "scores", "metadata", "categories"]
    for api in apis:
        s3_path = f's3://*******/{api}/{api}_{current_date}.json'
        data = read_json(s3_path)
        convert_to_csv(data,
                       f's3://*********/{api}/{api}_{current_date}.csv',
                       f's3://*********/{api}/{api}_{current_date}.csv')
        print("csv done")
        convert_to_parquet(data,
                           f's3://**********/{api}/{api}_{current_date}.parquet',
                           f's3://************/{api}/{api}_{current_date}.parquet')
        print("parquet done") 