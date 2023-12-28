import json
import boto3
import logging
# import pandas as pd

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
dynamodb_data_list = []


def craete_ddb_table(table):
    table = dynamodb.create_table(
    TableName=table,
    KeySchema=[
        {
            'AttributeName': 'brand',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'model',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'brand',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'model',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
    )


def add_single_row(table):
    response = table.put_item(
      Item={
            'brand': 'Tata',
            'model': 'Nano'
            
        }
    )

def insert_multiple_ddb(table):
    item_1 = {"brand":"Tayota", "model":"md1"}
    item_2 = {"brand":"Tayota", "model":"md2"}
    items_to_add = [item_1, item_2]
    with table.batch_writer() as batch:
        for item in items_to_add:
            response = batch.put_item(Item={
                "brand": item["brand"],
                "model": item["model"]
            })

def get_data_from_ddb(table):
    response = table.scan()
    items = response['Items']
    # logging.info("The items are:", items)
    # for i in items:
    #     model = item['model']
    #     brand = item['brand']
    #     new_item = (brand, model)
    #     dynamodb_data_list.append(tuple(new_item))
    # raw_df = pd.DataFrame(dynamodb_data_list, columns=['brand', 'model'])
    # print(raw_df)
    
def lambda_handler(event, context):
    # records = event
    print(event)
    table = dynamodb.Table('Car')
    table_name = 'Car'
    is_table_existing = table.table_status in ("CREATING", "UPDATING", "DELETING", "ACTIVE")
    if is_table_existing:
        print(f"Table {table_name} already exists")
    else:
        print(f"Table {table} not exists, creating now.")
        craete_ddb_table(table_name)
    add_single_row(table)
    insert_multiple_ddb(table)
    # get_data_from_ddb(table)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
=====================================================

import json
import os
import boto3
import csv


s3_resource = boto3.resource('s3')
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Employee_data")



def lambda_handler(event, context):
    key_name = 'Business-employment-data-september-2022-quarter-csv.csv'
    bucket_name = 'ak-test11'
    data = load_data_from_s3(key_name, bucket_name)
    craete_ddb_table(table)
    load_to_ddb(data)


def craete_ddb_table(table):
    table = dynamodb.create_table(
    TableName=table,
    KeySchema=[
        {
            'AttributeName': 'Series_reference',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'Period',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'Data_value',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'Suppressed',
            'AttributeType': 'S'
        },
		{
            'AttributeName': 'STATUS',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'UNITS',
            'AttributeType': 'S'
        },
		{
            'AttributeName': 'Magnitude',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'Subject',
            'AttributeType': 'S'
        },
		{
            'AttributeName': 'Group',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'Series_title_1',
            'AttributeType': 'S'
        },
		{
            'AttributeName': 'Series_title_2',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'Series_title_3',
            'AttributeType': 'S'
        },
		{
            'AttributeName': 'Series_title_4',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'Series_title_5',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
    )


def load_data_from_s3(key_name, bucket_name):
    # key_name = 'Business-employment-data-september-2022-quarter-csv.csv'
    # bucket_name = 'ak-test11'
    
    """
    s3_object = s3_resource.Object(bucket_name, key_name)
    
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
    
    lines = csv.reader(data)
    headers = next(lines)
    # print('headers: %s' %(headers))
    for line in lines:
        #print complete line
        print(line)
        #print index wise
        print(line[0], line[1])
    """
    resp = s3_client.get_object(Bucket=bucket_name,Key=key_name)
    data = resp['Body'].read().decode("utf-8")
    emp_data = data.split("\n")
    return emp_data
    
    # for friend in Students:
    #     # print(friend)
    #     friend_data = friend.split(",")
    #     print(friend_data[0])
    #     # add to dynamodb
    #     try:
    #         table.put_item(
    #             Item = {
    #                 "id"        : friend_data[0],
    #                 "name"      : friend_data[1],
    #                 "Subject"   : friend_data[2]
    #             }
    #         )
    #     except Exception as e:
    #         print("End of file")
 

def load_to_ddb(emp_data):
    
    for data in emp_data:
        # print(data)
        employee_data = data.split(",")
        print(employee_data[0])
        # add to dynamodb
        try:
            table.put_item(
                Item = {
                    "id"        : employee_data[0],
                    "name"      : employee_data[1],
                    "Subject"   : employee_data[2]
                }
            )
        except Exception as e:
            print("End of file")