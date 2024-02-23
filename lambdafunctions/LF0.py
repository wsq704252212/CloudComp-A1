import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('lex-runtime')

    print(event)

    response = client.post_text(
        botName='DiningSuggestion',
        botAlias='$LATEST',
        userId='default',
        inputText=json.loads(event['body'])['messages'][0]['unstructured']['text']
    )

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
        },
        'body': json.dumps({
            'messages' : [{
                'type' : 'unstructured',
                'unstructured' : {
                    'text' : response['message']
                }
            }]
        })
    }