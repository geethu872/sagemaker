import os
import boto3
import json
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
runtime = boto3.client('runtime.sagemaker')
sns = boto3.client('sns')

def lambda_handler(event, context):
    try:
        payload = json.loads(event['body'])  
        payload_data = payload.get('features') 

        if not payload_data:
            raise ValueError("Missing 'features' in the payload")

        
        payload_csv = ','.join(map(str, payload_data))

        # Invoke the SageMaker endpoint
        response = runtime.invoke_endpoint(
            EndpointName=ENDPOINT_NAME,
            ContentType='text/csv',  
            Body=payload_csv
        )

      
        result = json.loads(response['Body'].read().decode())  

       
        preds = {"Prediction": result}

       
        response_dict = {
            "statusCode": 200,
            "body": json.dumps(preds)
        }

        
        sns_message = json.dumps(response_dict)
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='Lambda Function Notification',
            Message=sns_message
        )

        return response_dict

    except Exception as e:
        
        error_message = str(e)
        response_dict = {
            "statusCode": 400,
            "body": json.dumps({"error": error_message})
        }

        sns_message = json.dumps(response_dict)
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='Lambda Function Error',
            Message=sns_message
        )

        return response_dict