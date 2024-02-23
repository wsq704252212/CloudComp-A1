from datetime import datetime, timezone
from dateutil import tz
import json
import boto3
import re

locations = ['new york', 'manhattan', 'brooklyn', 'queens', 'bronx', 'the bronx', 'staten island']
cuisines = ['chinese', 'japanese', 'thai', 'italian', 'mexican', 'indian', 'spanish', 'french']

def greet():
    return {
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': 'Fulfilled',
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help?'
            }
        }
    }

def suggest(event):
    name = 'DiningSuggestionsIntent'
    slots = event['currentIntent']['slots']

    location = slots['Location']
    cuisine = slots['Cuisine']
    date = slots['Date']
    time = slots['Time']
    people = slots['NumberOfPeople']
    email = slots['Email']

    utc_time = datetime.now(tz=timezone.utc)
    ny_time = utc_time.astimezone(tz.gettz('America/New_York'))
    today = ny_time.date().strftime("%Y-%m-%d")
    current_time = ny_time.time().strftime("%H:%M")

    print(today)
    print(current_time)

    if event['invocationSource'] == 'DialogCodeHook':
        if location is not None and location.lower() not in locations:
            return {
                'dialogAction': {
                    'type': 'ElicitSlot',
                    'message': {
                        'contentType': 'PlainText',
                        'content': 'We currently do not support that location. Please enter a location in NYC.'
                    },
                    'intentName': name,
                    'slots': slots,
                    'slotToElicit': 'Location'
                }
            }

        if cuisine is not None and cuisine.lower() not in cuisines:
            cuisinesTitled = ', '.join([cuisine.title() for cuisine in cuisines])
            return {
                'dialogAction': {
                    'type': 'ElicitSlot',
                    'message': {
                        'contentType': 'PlainText',
                        'content': f'We currently do not support that cuisine. Please choose a cuisine style among {cuisinesTitled}.'
                    },
                    'intentName': name,
                    'slots': slots,
                    'slotToElicit': 'Cuisine'
                }
            }

        if date is not None and date < today:
            return {
                'dialogAction': {
                    'type': 'ElicitSlot',
                    'message': {
                        'contentType': 'PlainText',
                        'content': 'The date cannot be earlier than the current date. Please provide a valid date.'
                    },
                    'intentName': name,
                    'slots': slots,
                    'slotToElicit': 'Date'
                }
            }

        if time is None and event['currentIntent']['slotDetails'] is not None and event['currentIntent']['slotDetails']['Time'] is not None:
            resolutions = event['currentIntent']['slotDetails']['Time']['resolutions']
            return {
                'dialogAction': {
                    'type': 'ElicitSlot',
                    'message': {
                        'contentType': 'PlainText',
                        'content': f'I’m not sure what time you are asking for. Did you mean {resolutions[0]['value']} or {resolutions[1]['value']}?'
                    },
                    'intentName': name,
                    'slots': slots,
                    'slotToElicit': 'Time'
                }
            }

        if date == today and time is not None and time < current_time:
            return {
                'dialogAction': {
                    'type': 'ElicitSlot',
                    'message': {
                        'contentType': 'PlainText',
                        'content': 'The time cannot be earlier than the current time. Please provide a valid time.'
                    },
                    'intentName': name,
                    'slots': slots,
                    'slotToElicit': 'Time'
                }
            }

        if people is not None and (not people.isnumeric() or int(people) < 1 or int(people) > 20):
            return {
                'dialogAction': {
                    'type': 'ElicitSlot',
                    'message': {
                        'contentType': 'PlainText',
                        'content': 'The number of people must be between 1 and 20. Please provide a valid number of people.'
                    },
                    'intentName': name,
                    'slots': slots,
                    'slotToElicit': 'NumberOfPeople'
                }
            }

        if email is not None and not re.fullmatch(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b', email):
            return {
                'dialogAction': {
                    'type': 'ElicitSlot',
                    'message': {
                        'contentType': 'PlainText',
                        'content': 'The email is not in the correct format. Please provide a valid email.'
                    },
                    'intentName': name,
                    'slots': slots,
                    'slotToElicit': 'Email'
                }
            }

        return {
            'dialogAction': {
                'type': 'Delegate',
                'slots': slots
            }
        }

    # event['invocationSource'] == 'FulfillmentCodeHook'
    sqs = boto3.client("sqs")
    queue = 'https://sqs.us-east-1.amazonaws.com/424508226690/chatbot.fifo'
    message = json.dumps({
        'Location': location,
        'Cuisine': cuisine,
        'Date': date,
        'Time': time,
        'NumberOfPeople': people,
        'email': email
    })
    sqs.send_message(
        QueueUrl=queue,
        MessageBody=message,
        MessageGroupId='0'
    )

    return {
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': 'Fulfilled',
            'message': {
                'contentType': 'PlainText',
                'content': f'You’re all set. Expect my suggestions for {cuisine} food in {location} on {date} at {time} for {people} people shortly!'
            }
        }
    }

def thank():
    return {
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': 'Fulfilled',
            'message': {
                'contentType': 'PlainText',
                'content': 'You’re welcome. Have a good day!'
            }
        }
    }

def lambda_handler(event, context):
    print(event)

    intent = event['currentIntent']['name']

    if intent == 'GreetingIntent':
        return greet()
    if intent == 'DiningSuggestionsIntent':
        return suggest(event)
    if intent == 'ThankYouIntent':
        return thank()

    return {
        'dialogAction': {
            'type': 'ElicitIntent',
            'message': {
                'contentType': 'PlainText',
                'content': 'I’m sorry, I don’t understand. Can you please rephrase?'
            }
        }
    }