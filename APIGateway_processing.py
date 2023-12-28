import json

def lambda_handler(event, context):
    # TODO implement
    transactionId = event['queryStringParameters']['transactionId']
    transactionType = event['queryStringParameters']['type']
    transactionAmount = event['queryStringParameters']['amount']
    print("TransactionID is="+ transactionId)
    print("Transaction type is=", transactionType)
    print("Transaction amount is=", transactionAmount)

    transactionResponse = {}
    transactionResponse['transactionId'] = transactionId
    transactionResponse['type'] = transactionType
    transactionResponse['mount'] = transactionAmount
    transactionResponse['message'] = "Here from Lambda"
    
    responseObject = {}
    responseObject['statusCode'] = 200
    responseObject['headers']={}
    responseObject['headers']['content-type'] = 'application/json'
    responseObject['body'] = json.dumps(transactionResponse)
    
    return responseObject
    
    
    