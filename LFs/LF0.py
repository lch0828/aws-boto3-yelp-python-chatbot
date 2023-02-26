import boto3
# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')
def lambda_handler(event, context):
    # change this to the message that user submits on 
    # your website using the 'event' variable
    print(event)
    msg_from_user = event['messages'][0][event['messages'][0]['type']]['text']
    print(f"Message from frontend: {msg_from_user}")
    
    # Initiate conversation with Lex
    response = client.recognize_text(
            botId='POJJHFPB6E', # MODIFY HERE
            botAliasId='U6AFCBVCKU', # MODIFY HERE
            localeId='en_US',
            sessionId='testuser',
            text=msg_from_user)
    
    msg_from_lex = response.get('messages', [])
    print(msg_from_lex)
    if msg_from_lex:
        print(f"Message from Chatbot: {msg_from_lex[0]['content']}")
        print(response)
        
        text = []
        for msg in msg_from_lex:
            content = {}
            content['type'] = 'unstructured'
            content['unstructured'] = {'text': msg['content']}
            text.append(content)
            
        print(text)
        
        resp = {
            'statusCode': 200,
            'body': "Hello from LF0!",
            'messages': text
        }

        return resp
