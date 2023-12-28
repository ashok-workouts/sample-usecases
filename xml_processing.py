import json
import boto3
import pathlib

import pandas as pd
import xml.etree.ElementTree as ET


bucket_name = 'work-sample-us-east-1'
prefix = 'data/'
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def get_latest_file(bucket_name, prefix):
    
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    all = response['Contents']        
    latest = max(all, key=lambda x: x['LastModified'])
    file = latest['Key'].split('/')[1]
    return file

def property_data1(tab_name, file):
    xml_data = open(file, 'r').read()  # Read file
    root = ET.XML(xml_data)  # Parse XML
    data = []
    cols = []
    for i, child in enumerate(root):
        data.append([subchild.text for subchild in child])
        cols.append(child.tag)
    
    property_df = pd.DataFrame(data).T  # Write in DF and transpose it
    property_df.columns = cols  # Update column names
    table = dynamodb.Table(tab_name)
    with table.batch_writer(overwrite_by_pkeys=["house_type", "property_id"]) as bw:
            for record in property_df.to_dict("records"):
                bw.put_item(Item=record)


def book_data(tab, file):
    print("we are in book_data.....")
    books_df = pd.read_xml('xml_files/books.xml', xpath=".//book")
    print(books_df)
    table = dynamodb.Table(tab)
    with table.batch_writer(overwrite_by_pkeys=["id", "author"]) as bw:
            for record in books_df.to_dict("records"):
                bw.put_item(Item=record)


def shape_data(tab, file):
    print("we are in shape_data.....")
    shape_df = pd.read_xml('xml_files/shapes.xml', xpath="//doc:row", namespaces={"doc": "https://example.com"})
    shape_df['sides'] = shape_df['sides'].astype(int)
    print(shape_df)
    table = dynamodb.Table(tab)
    with table.batch_writer(overwrite_by_pkeys=["shape", "degrees"]) as bw:
            for record in shape_df.to_dict("records"):
                bw.put_item(Item=record)


def lambda_handler(event, context):

    file_name = get_latest_file(bucket_name, prefix)
    file_extension = pathlib.Path(file_name).suffix
    tab = file_name.split('.')[0]
    if file_extension == '.xml':
        print(f"it is xml and file: {file_name} and table name is: {tab}")
    if tab == "property_data":
        property_data(tab, file_name)
    if tab == "book_data":
        book_data(tab, file_name)
    if tab == "Shapes":
        shape_data(tab, file_name)


def property_data(tab_name, file):
    print("we are in property_data.....")
    paginator = s3_client.get_paginator('list_objects_v2')
    result = paginator.paginate(
        Bucket='work-sample-us-east-1',
        Prefix='data/')
    
    bucket_object_list = []
    
    for page in result:
        if "Contents" in page:
            for key in page["Contents"]:
                keyString = key["Key"]
                bucket_object_list.append(keyString)
    
    for file_name in bucket_object_list:
        obj = s3_resource.Object('work-sample-us-east-1', file_name)
        xmldata = obj.get()["Body"].read().decode('utf-8')

    root = ET.XML(xmldata)  # Parse XML
    data = []
    cols = []
    for i, child in enumerate(root):
        data.append([subchild.text for subchild in child])
        cols.append(child.tag)
    
    property_df = pd.DataFrame(data).T  # Write in DF and transpose it
    property_df.columns = cols  # Update column names
    print(property_df)
    table = dynamodb.Table(tab_name)
    with table.batch_writer(overwrite_by_pkeys=["house_type", "property_id"]) as bw:
            for record in property_df.to_dict("records"):
                bw.put_item(Item=record)
