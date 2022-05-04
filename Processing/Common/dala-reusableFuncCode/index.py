import os
import json
from  urllib.parse import unquote_plus
import boto3

print('Loading function')

def lambda_handler(event, context):

    data = {
     # Get the object from the event and show it
     #General
     'awsRegion' : event['Records'][0]['awsRegion'],
     'eventVersion' : event['Records'][0]['eventVersion'],
     'eventSource' : event['Records'][0]['eventSource'],
     'eventType' : event['Records'][0]['eventName'],
     'eventTime' : event['Records'][0]['eventTime'],
     'userIdentity' : event['Records'][0]['userIdentity']['principalId'],
     'sourceIPAddress' : event['Records'][0]['requestParameters']['sourceIPAddress'],
     'requestId' : event['Records'][0]['responseElements']['x-amz-request-id'], #PK
     'requestId2' : event['Records'][0]['responseElements']['x-amz-id-2'],
     #s3
     'bucket' : event['Records'][0]['s3']['bucket']['name'],
     'bucketArn' : event['Records'][0]['s3']['bucket']['arn'],
     'key' : unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8'),
     'objVersionId' : event['Records'][0]['s3']['object'].get('versionId'),
     'objSequencer' : event['Records'][0]['s3']['object'].get('sequencer'),
     'objSizeBytes' : event['Records'][0]['s3']['object'].get('size'),
     #Get attributes from context
     'awsLogGroupName' : context.log_group_name ,
     'awsLogStreamName' : context.log_stream_name 
    }
    
    #print(json.dumps(data))
    
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['DynDBTableName'])
        table.put_item(Item = data)
        print('Event data has been successfully loaded')
    except Exception as e:
        print(e)   
