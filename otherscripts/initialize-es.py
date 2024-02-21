from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

# create es on aws before execution

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

# type is dedeprecated in es 7.10
resp = es.indices.create(
    index="restaurants",
    body={
        "mappings": {
            "properties": {
                "BusinessID": { "type": "text" },
                "Cuisine": { "type": "text" }
            }
        }
    },
)

print(resp)