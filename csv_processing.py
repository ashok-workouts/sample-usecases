import json
import os
import boto3
import csv
import sys
import pandas as pd
import awswrangler as wr
import datetime as dt 

from decimal import Decimal
from typing import Optional

s3_resource = boto3.resource('s3')
s3_client = boto3.client("s3")
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
# table = dynamodb.Table("Employee_data1")
# table_name = 'Employee_data1'
table = dynamodb.Table("employee")
table_name = 'employee'
sys.setrecursionlimit(7000)
dynamodb_data_list = []


def lambda_handler(event, context):

    # is_table_existing = table.table_status in ("CREATING", "UPDATING", "DELETING", "ACTIVE")
    # if is_table_existing:
    #     print(f"Table {table_name} already exists")
    # else:
    #     print(f"Table {table} not exists, creating now.")
    #     craete_ddb_table(table_name)

    bucket_name = 'work-sample-us-east-1'
    file_name = 'Two_fields.csv'
    # file_name = 'October-2022_data.csv'
    # read_from_s3_load_ddb(file_name, bucket_name)
    get_data_from_ddb(table)

def get_data_from_ddb(table):
    response = table.scan()
    items = response['Items']
    for item in items:
        series = item['Series_reference']
        data = item['Data_value']
        period = item['Period']
        new_item = (series, data, period)
        dynamodb_data_list.append(tuple(new_item))
    raw_df = pd.DataFrame(dynamodb_data_list, columns=['series', 'data', 'period'])
    print(len(raw_df.index))
    
    
def date_converter(obj):
    if isinstance(obj, dt.datetime):
        return obj.__str__()
    elif isinstance(obj, dt.date):
        return obj.isoformat()


def read_from_s3_load_ddb(file_name, bucket_name):

    # Load CSV file from S3
    file_path = f's3://{bucket_name}/{file_name}'
    df = wr.s3.read_csv(path=file_path, sep=',', na_values=['null', 'none'], skip_blank_lines=True)
    print(df.dtypes.values)
    print(df.dtypes)

    # Check if they are floats (convert to decimals instead) 
    if any([True for v in df.dtypes.values if v=='float64']):

        # Save decimals with JSON
        df_json = json.loads(
                       json.dumps(df.to_dict(orient='records'),
                                  default=date_converter,
                                  allow_nan=True), 
                       parse_float=Decimal
                       )

        # Batch write 
        with table.batch_writer(overwrite_by_pkeys=["Series_reference", "Data_value"]) as batch: 
            for element in df_json:
                batch.put_item(Item=element)
    else:
        print("we are in else===================")
        with table.batch_writer(overwrite_by_pkeys=["Series_reference", "Data_value"]) as bw:
            for record in df.to_dict("records"):
                bw.put_item(Item=record)
