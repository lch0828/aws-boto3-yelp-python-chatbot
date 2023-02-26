import json
import os
import boto3
from botocore.exceptions import ClientError
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

REGION = 'us-east-1'
HOST = 'search-restaurants-xrohjixqi37i3sy26ergjiw3oa.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'

def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event))
    
    queue = boto3.client('sqs')
    URL = 'https://sqs.us-east-1.amazonaws.com/509456314600/CloudHw1'
    msg = receive_messages(queue, URL)
    
    if 'Messages' in msg:
        msg = msg['Messages'][0]
        cuisine = msg['MessageAttributes']['Cuisine']['StringValue']
        
        # ElasticSearch
        results = query(cuisine)
        
        # Query DynamoDB
        items = []
        for item in results:
            items.append(lookup_data({'BusinessID': item['BusinessID']}))
            
        message_to_usr = f"Hello! Here are my {len(items)} restaurant suggestions for {msg['MessageAttributes']['PeopleNum']['StringValue']} people, for {msg['MessageAttributes']['DiningDate']['StringValue']} at {msg['MessageAttributes']['DiningTime']['StringValue']}: "
        for i in range(len(items)):
            message_to_usr += f"{i+1}. {items[i]['Name']}, located at {items[i]['Address']}"
            if i != len(items):
                message_to_usr += ', '
            else:
                message_to_usr += '. '
        message_to_usr += 'Enjoy your meal!'
        
        # Send SNS
        print(message_to_usr)
        send_email(msg['MessageAttributes']['Email']['StringValue'], message_to_usr)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        }
    }
    
def query(term):
    q = {'size': 5, 'query': {'multi_match': {'query': term, 'fields': ['Cuisine']}}}
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)
    res = client.search(index=INDEX, body=q)
    print(res)
    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])
    return results
    
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
                    
def receive_messages(queue, url, max_number=1, wait_time=10):
    """
    Receive a batch of messages in a single request from an SQS queue.

    :param queue: The queue from which to receive messages.
    :param max_number: The maximum number of messages to receive. The actual number
                       of messages received might be less.
    :param wait_time: The maximum time to wait (in seconds) before returning. When
                      this number is greater than zero, long polling is used. This
                      can result in reduced costs and fewer false empty responses.
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
    try:
        messages = queue.receive_message(
            QueueUrl=url,
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
        )
        print('messages: ', messages)
    except ClientError as error:
        print("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        if "Messages" in messages:
            response = queue.delete_message(
                QueueUrl=url,
                ReceiptHandle=messages['Messages'][0]['ReceiptHandle']
            )
        return messages

def lookup_data(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response['Item'])
        return response['Item']
        
def send_email(mail, message):
    SENDER = "mjbl8228@gmail.com"
    RECIPIENT = mail
    AWS_REGION = "us-east-1"
    SUBJECT = "Restaurant Suggestions"
    BODY_TEXT = message
    CHARSET = "UTF-8"
    
    client = boto3.client('ses',region_name=AWS_REGION)
    
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

