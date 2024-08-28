import json
import boto3
import awswrangler as wr

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    df = wr.s3.read_csv(path=["s3://work-us-east-1/rawdata/customers.csv"])
    print(df)
    
    # ex_df = wr.s3.read_excel("s3://work-us-east-1/rawdata/Inventory-Records-Sample-Data.xlsx")
    # print(ex_df)

    # wr.s3.to_csv(df=df, path="s3://work-us-east-1/rawdata/customers_dup.csv")
    # wr.s3.to_excel(df=ex_df, path="s3://work-us-east-1/rawdata/Inventory-Records-Sample-Data_dup.xlsx")
    # wr.s3.to_parquet(df=ex_df, path="s3://work-us-east-1/rawdata/Inventory-Records-Sample-Data.parquet")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



import json
import awswrangler as wr

def lambda_handler(event, context):
    # TODO implement
    # df1 = wr.s3.read_csv("s3://sample-work-us-east-1/customers.csv")
    # wr.s3.to_csv(df=df1, path="s3://sample-work-us-east-1/customers_duplicate.csv")
    ex_df = wr.s3.read_excel("s3://sample-work-us-east-1/Inventory-Records-Sample-Data.xlsx")
    # print(ex_df)
    # wr.s3.to_excel(df=ex_df, path="s3://sample-work-us-east-1/Inventory-Records-Sample-Data_duplicate.xlsx")
    wr.s3.to_parquet(df=ex_df, path="s3://sample-work-us-east-1/Inventory-Records-Sample-Data.parquet")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
