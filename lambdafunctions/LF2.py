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
    if businessID == '':
        return False
    
    db = boto3.resource('dynamodb')
    table = db.Table("yelp-restaurants")
    resp = table.query(KeyConditionExpression=Key("BusinessID").eq(businessID))

    if len(resp["Items"]) == 0: 
        return False
    
    SendEmail(email, resp["Items"][0])
    return True


def ReceiveSQS():
    sqs = boto3.client('sqs')

    queue_url = 'https://sqs.us-east-1.amazonaws.com/424508226690/chatbot.fifo'

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']

    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    print('Received and deleted message: %s' % message)

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

    if data['hits']['hits']:
        return data['hits']['hits'][0]['_source']['BusinessID']
    else:
        return ''



def SendEmail(email, restaurant):
    sesClient = boto3.client('ses')
    
    message = "<p>Hello!</p><p>Successfully reserve seats in the restaurant <b>{resNmae}</b>.<p> \
    <p>Location: {addr}</p><p>Rating: {rating}<p>".format(resNmae = restaurant['Name'], addr = restaurant['Address'], rating = restaurant['Rating'])
    
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