import boto3
import json
from decimal import Decimal

def convert_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {key: convert_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_decimal(item) for item in obj]
    return obj

def get_json_data_from_s3(bucket_name,prefix,aws_access_key_id,aws_secret_access_key,aws_region):

    # Initialize S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
    
    response = s3.list_objects_v2(Bucket=bucket_name,Prefix=prefix)

    json_data_dict = {}

    for obj in response.get('Contents', []):
        file_key = obj['Key']
    
    # Download the JSON file content
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
    
        try:
            # Parse the JSON content and store in the dictionary
            json_data = json.loads(content)
            json_data_dict[file_key] = json_data
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for file {file_key}: {e}")

    return json_data_dict


def load_data_into_dynamodb(data_dict, table_name, aws_access_key_id,aws_secret_access_key,aws_region):
    # Create a DynamoDB resource
    dynamodb = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,region_name=aws_region)

    # Specify the DynamoDB table
    table = dynamodb.Table(table_name)

    # Iterate through items in the data dictionary and put them into DynamoDB
    with table.batch_writer() as batch:
        for file_key, json_data in data_dict.items():
            # Convert all numeric values to Decimal
            json_data_decimal = convert_to_decimal(json_data)
            batch.put_item(Item=json_data_decimal)

    print(f"Data loaded into DynamoDB table {table_name}.")


if __name__ == '__main__':

    aws_access_key_id = 'AKIAQH6JCFVUTA4IDD3Q'
    aws_secret_access_key = '6/W9VhQYubN5jQStoL5cbzuImienm0igZolmAhYi'
    aws_region = 'us-east-1'  # Replace with your desired AWS region
    bucket_name = 'guvifinalproject'

    # Specify the prefix (folder) in the S3 bucket where your JSON files are stored
    prefix = 'extracted/test/'

    # Method to read data from s3 bucket and store into data dictionary
    result_dict = get_json_data_from_s3(bucket_name,prefix,aws_access_key_id,aws_secret_access_key,aws_region)
    #print(result_dict)

    # Load s3 json file data into Dynamodb
    dynamodb_table_name = 'dataectraction'
    load_data_into_dynamodb(result_dict, dynamodb_table_name, aws_access_key_id,aws_secret_access_key,aws_region)