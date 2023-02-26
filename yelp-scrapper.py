import json
import boto3
from botocore.exceptions import ClientError
import requests
import datetime
from keys import yelp_key
CUISINE=['Italian', 'Japanese', 'French', 'Thai', 'Indian', 'Mexican', 'Spanish', 'Korean', 'Latin', 'MiddleEastern']
KEY = yelp_key #create keys.py and store yelp api key as yelp_key

def lambda_handler(event, context):
    for c in CUISINE:
        for i in range(0, 950, 50):
            url = f"https://api.yelp.com/v3/businesses/search?location=NYC&term={c}&categories=&sort_by=best_match&limit=50&offset={i}"
        
            headers = {
                "accept": "application/json",
                "Authorization": "Bearer" + KEY 
            }
            
            response = requests.get(url, headers=headers)
            response = json.loads(response.text)
            result = []
            
            for r in response["businesses"]:
                restaurant = {}
                restaurant['BusinessID'] = r['id']
                restaurant['Name'] = r['name']
                restaurant['Cuisine'] = r['categories'][0]['title']
                restaurant['Address'] = r['location']['address1']
                restaurant['latitude'] = str(r['coordinates']['latitude'])
                restaurant['longitude'] = str(r['coordinates']['longitude'])
                restaurant['ReviewsNum'] = r['review_count']
                restaurant['Rating'] = str(r['rating'])
                restaurant['Zip'] = r['location']['zip_code']
                restaurant['insertedAtTimestamp'] = str(datetime.datetime.now())
                
                result.append(restaurant)
            
            insert_data(result)
    return

def insert_data(data_list, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # overwrite if the same index is provided
    for data in data_list:
        response = table.put_item(Item=data)
    print('@insert_data: response', response)
    return response

