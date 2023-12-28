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
    print("======event is==========: ", event)
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