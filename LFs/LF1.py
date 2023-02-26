from datetime import datetime, date
import boto3
from botocore.exceptions import ClientError

SLOTS = ['Location','Cuisine','PeopleNum','DiningDate','DiningTime','Email']

def lambda_handler(event, context):
    msg_from_user = event['inputTranscript']
    user_intent = event['sessionState']['intent']['name']
    print(f"Message from frontend: {msg_from_user}")
    print(event)
    
    #Greetingintent
    if user_intent == 'Greetingintent':
        return Greetingintent()
        
    #DiningSuggestionsIntent
    elif user_intent == 'DiningSuggestionsIntent':
        return DiningSuggestionsIntent(event)
        
    elif user_intent == 'ThankYouIntent':
        return ThankYouIntent()
        
def Greetingintent():
    response = {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitIntent"
            }
        },
        "messages": [
            {
            "contentType": "PlainText",
            "content": "Hello. What can I help you with?"
            }
        ]
    }
    return response
    
def ThankYouIntent():
    response = {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitIntent"
            }
        },
        "messages": [
            {
            "contentType": "PlainText",
            "content": "You're welcome!"
            }
        ]
    }
    return response
        
def DiningSuggestionsIntent(event):
    slots = event['sessionState']['intent']['slots']
    print(slots)
    if slots['DiningDate'] != None and len(slots['DiningDate']['value']['resolvedValues']) == 1:
        date_in = datetime.strptime(slots['DiningDate']['value']['resolvedValues'][0], '%Y-%m-%d')
        if slots['DiningTime'] != None and len(slots['DiningTime']['value']['resolvedValues']) == 1:
            time = datetime.strptime(slots['DiningDate']['value']['resolvedValues'][0] + slots['DiningTime']['value']['resolvedValues'][0], '%Y-%m-%d%H:%M')
            if time < datetime.now():
                return slotResponse('DiningTime', slots)
                
        if date_in.date() < date.today():
            return slotResponse("DiningDate", slots)
        
            
    for key in SLOTS:
        if (slots[key] != None and len(slots[key]['value']['resolvedValues']) != 1) or slots[key] == None:
            return slotResponse(key, slots)
    
    response = {
        "sessionState": {
            "dialogAction": {
                "type": "Delegate"
            },
            "intent": {
                "name": "DiningSuggestionsIntent",
                "slots": slots
            }
        }
    }
    
    if event['invocationSource'] == 'FulfillmentCodeHook':
        queue = boto3.client('sqs')
        URL = 'https://sqs.us-east-1.amazonaws.com/509456314600/CloudHw1'
        attr = {}
        attr['PeopleNum'] = {'StringValue': slots['PeopleNum']['value']['resolvedValues'][0], 'DataType': 'String'}
        attr['DiningDate'] = {'StringValue': slots['DiningDate']['value']['resolvedValues'][0], 'DataType': 'String'}
        attr['DiningTime'] = {'StringValue': slots['DiningTime']['value']['resolvedValues'][0], 'DataType': 'String'}
        attr['Cuisine'] = {'StringValue': slots['Cuisine']['value']['resolvedValues'][0], 'DataType': 'String'}
        attr['Email'] = {'StringValue': slots['Email']['value']['resolvedValues'][0], 'DataType': 'String'}
        send_message(queue, URL, 'suggestionRequest', attr)
        
        response = {
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": "DiningSuggestionsIntent",
                "slots": slots,
                "state": "Fulfilled"
            }
        },
        "messages": [
            {
            "contentType": "PlainText",
            "content": "You’re all set. Expect my suggestions shortly! Have a good day."
            }
        ]
    }
    
    return response
            
def slotResponse(key, slots):
    response = {
        "sessionState": {
            "dialogAction": {
                "slotToElicit": key,
                "type": "ElicitSlot"
            },
            "intent": {
                "name": "DiningSuggestionsIntent",
                "slots": slots
            }
        }
    }
    return response
    
def send_message(queue, url, message_body, message_attributes=None):
    """
    Send a message to an Amazon SQS queue.

    :param queue: The queue that receives the message.
    :param message_body: The body text of the message.
    :param message_attributes: Custom attributes of the message. These are key-value
                               pairs that can be whatever you want.
    :return: The response from SQS that contains the assigned message ID.
    """
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            QueueUrl=url,
            MessageBody=message_body,
            MessageAttributes=message_attributes
        )
    except ClientError as error:
        print("Send message failed: %s", message_body)
        raise error
    else:
        return response
        
    
