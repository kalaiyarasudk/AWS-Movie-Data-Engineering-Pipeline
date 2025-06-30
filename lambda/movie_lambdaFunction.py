import json
import urllib.parse
import boto3

client = boto3.client('stepfunctions')
STEP_FUNCTION_ARN = 'arn:aws:states:eu-north-1:718226529369:stateMachine:movies_stepfunction'

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        try:
            response = client.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                input=json.dumps({'bucket': bucket, 'key': key})
            )
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
