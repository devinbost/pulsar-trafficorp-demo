import boto3
import datetime
import tweepy
import base64
import hashlib
import json
import requests
from requests_oauthlib import OAuth1

def get_twitter_keys() -> dict:
    """Retrieve secrets from Parameter Store."""
    # Create our SSM Client.
    aws_client = boto3.client('ssm')

    # Get our keys from Parameter Store.
    parameters = aws_client.get_parameters(
        Names=[
            'twitter_api_key',
            'twitter_api_key_secret',
            'twitter_client_token',
            'twitter_client_token_secret'
        ],
        WithDecryption=True
    )

    # Convert list of parameters into simpler dict.
    keys = {}
    for parameter in parameters['Parameters']:
        keys[parameter['Name']] = parameter['Value']

    return keys

def connect_to_oauth(consumer_key, consumer_secret, acccess_token, access_token_secret):
   url = "https://api.twitter.com/2/tweets"
   auth = OAuth1(consumer_key, consumer_secret, acccess_token, access_token_secret)
   return url, auth

def lambda_handler(event, context):
    """Main Lambda function."""
    keys = get_twitter_keys()

    client = tweepy.Client(
        consumer_key=keys.get('twitter_api_key'),
        consumer_secret=keys.get('twitter_api_key_secret'),
        access_token=keys.get('twitter_client_token'),
        access_token_secret=keys.get('twitter_client_token_secret')

    )
    #url, auth = connect_to_oauth(
    #   keys.get('twitter_api_key'), keys.get('twitter_api_key_secret'), keys.get('twitter_client_token'), keys.get('twitter_client_token_secret')
    #)
    for record in event['Records']:
        #Kinesis data is base64 encoded so decode here
        payload=base64.b64decode(record["kinesis"]["data"])
        msg = str(payload)
        print("Decoded payload: " + msg)
        asAscii = payload.decode('ascii')
        print("Decoded as ascii: " + asAscii)
        parsed = json.loads(asAscii)
        print("IssueReported: " + parsed["IssueReported"])
        tweetText = parsed["IssueReported"] + " at location: " + parsed["Location"]
        tweetJson = '{"text": "' + tweetText + '"}'
        #request = requests.post(
        #        auth=auth, url=url, json=, headers={"Content-Type": "application/json"}
        #)    
        response = client.create_tweet(text=tweetJson)
        print(response)
