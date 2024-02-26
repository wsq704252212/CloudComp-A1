import json
import boto3
from boto3.dynamodb.conditions import Key
import requests

# pip install requests -t requests
# Upload request pakcage with lambda_code to aws

# Fill your elasticSearch account first.
esaccount = ("", "")

def lambda_handler(event, context):
    queue_url = 'https://sqs.us-east-1.amazonaws.com/424508226690/chatbot.fifo'
    sqs = boto3.client('sqs')
    messages = readSQS(sqs, queue_url)

   # messages = {"Body": "{\"Cuisine\":\"chinese\",\"email\":\"sw6195@nyu.edu\"}"}
    for m in messages:
        body = json.loads(m['Body'])

        isSuccess = worker(body)
        if isSuccess:
            receipt_handle = m['ReceiptHandle']
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )

    return {
        'statusCode': 200,
        'body': json.dumps('Done')
    }


def readSQS(sqsClient, queue_url):
    # Receive message from SQS queue
    response = sqsClient.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=3,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=30,
        WaitTimeSeconds=0
    )
    if 'Messages' not in response:
        return []
    
    return response['Messages']


def worker(body):
    cuisine = body["Cuisine"]
    email = body["email"]

    businessID = GetBusinessIDFromES(cuisine)
    if len(businessID) == 0:
        return False
    
    db = boto3.resource('dynamodb')
    table = db.Table("yelp-restaurants")

    candidates = []
    for i, id in enumerate(businessID):
        resp = table.query(KeyConditionExpression=Key("BusinessID").eq(id))
        if len(resp["Items"]) != 0: 
            candidates.append(resp["Items"][0])


    if len(candidates) == 0: 
        return False
    
    SendEmail(email, candidates)
    return True

def GetBusinessIDFromES(cuisine):
    query = {
        "query": {
            "function_score": {
                "query": {"match": {"Cuisine": cuisine}},
                "functions": [
                    {
                        "random_score": {}
                    }
                ],
                "score_mode": "sum"
            }
        }
    }  
    url = "https://search-restaurants-fbdaafwk7vn54dct2nus5gsfwq.us-east-1.es.amazonaws.com/restaurants/_search"
    resp = requests.get(url, auth=esaccount, json=query)    
    data = json.loads(resp.content.decode())

    bid = []
    if data['hits']['hits']:
        resNum = len(data['hits']['hits'])
        if resNum > 3:
            resNum = 3
        for i in range(resNum):
            bid.append(data['hits']['hits'][i]['_source']['BusinessID'])
    return bid



def SendEmail(email, restaurants):
    sesClient = boto3.client('ses')
    
    message = "<p>Hello!</p><p>Successfully recommand restaurant.</p>"
    
    for res in restaurants:
        message = message + "<p><b>{resNmae}</b>, Location: {addr}, Rating: {rating}</p> " \
            .format(resNmae = res['Name'], addr = res['Address'], rating = res['Rating'])
    
    send_args = {
        "Source": "siqiwan1997@gmail.com",
        "Destination": {"ToAddresses": [email]},
        "Message": {
                "Subject": {"Data": "Restaurant Reservation"},
                "Body": {"Html": {"Data": message}},
        },
    }

    response = sesClient.send_email(**send_args)
    message_id = response["MessageId"]
    print("Sent mail %s from %s to %s.", message_id, "siqiwan1997@gmail.com", email)