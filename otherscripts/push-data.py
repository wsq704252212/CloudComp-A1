import requests
import json
import boto3
import time
import decimal
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# create es and dynamodb on aws and execute initialize-es.py before execution
# Fill your yelp apiKey first
apiKey = ""

# create es client
session = boto3.Session()
crd = session.get_credentials()
crd = crd.get_frozen_credentials()

credentials = {
    'access_key': crd.access_key,
    'secret_key': crd.secret_key
}

host = 'search-restaurants-fbdaafwk7vn54dct2nus5gsfwq.us-east-1.es.amazonaws.com'
region = 'us-east-1'
service = 'es'
awsauth = AWS4Auth(credentials['access_key'], credentials['secret_key'], region, service)

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

# create db client
db = boto3.resource('dynamodb')
table = db.Table("yelp-restaurants")

# initial yelp api
headers = {
    "accept": "application/json",
    "Authorization": "Bearer " + apiKey
}

categories = ["chinese", "french", "british"]

# push data
for c in categories:
    url = "https://api.yelp.com/v3/businesses/search?location=Manhattan&term=restaurants&categories=" + c + "&sort_by=best_match&limit=20"
    response = requests.get(url, headers=headers)
    parsed = json.loads(response.text)

    for res in parsed["businesses"]:
        resItem = {
            'BusinessID': res['id'],
            'Name': res['name'],
            'Cuisine': c, 
            'Address': res['location']['address1'],
            'Coordinates': res['coordinates'],
            'NumberOfReviews': res['review_count'],
            'Rating': res['rating'],
            'ZipCode': res['location']['zip_code'],
            'InsertedAtTimestamp': round(time.time()*1000)
        }
        
        dbItem = json.loads(json.dumps(resItem), parse_float=decimal.Decimal)
        table.put_item(Item=dbItem)

        document = {
            'BusinessID': res['id'],
            'Cuisine': c
        }
        es.index(index='restaurants', body=document)

print(f"Put items succeeded.")




