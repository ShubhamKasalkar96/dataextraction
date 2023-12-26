import boto3
import json
from decimal import Decimal
from botocore.exceptions import ClientError

def batch_write_with_backoff(dynamodb_client, request_params):
    max_retries = 5
    base_delay = 0.1  # Initial delay in seconds

    for i in range(max_retries):
        try:
            response = dynamodb_client.batch_write_item(**request_params)
            return response
        except ClientError as e:
            if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                delay = base_delay * (2 ** i)
                time.sleep(delay)
            else:
                raise  # Propagate other errors

    raise Exception("Max retries reached. Unable to complete BatchWriteItem operation.")



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


def does_table_exist(table_name, dynamodb_client):
    try:
        dynamodb_client.describe_table(TableName=table_name)
        return True
    except dynamodb_client.exceptions.ResourceNotFoundException:
        return False


def create_dynamodb_table(table_name, partition_key_name, partition_key_type, dynamodb_client):
    response = dynamodb_client.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': partition_key_name, 'KeyType': 'HASH'}  # Partition key
        ],
        AttributeDefinitions=[
            {'AttributeName': partition_key_name, 'AttributeType': partition_key_type}
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    print("Table created:", response)


def insert_data_into_dynamodb(table_name, data, dynamodb_client,aws_region,aws_access_key_id,aws_secret_access_key):
    table = boto3.resource('dynamodb',region_name=aws_region,aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key).Table(table_name)

    with table.batch_writer() as batch:
        for file_key, json_data in data.items():
            # Convert all numeric values to Decimal
            json_data_decimal = convert_to_decimal(json_data)
            batch.put_item(Item=json_data_decimal)

    print("Data inserted into DynamoDB table.")




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

    table_name = 'dataextraction'
    partition_key_name = 'cik'
    partition_key_type = 'N'  # String, change to 'N' for Number

    dynamodb_client = boto3.client('dynamodb', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Check if the table exists
    if does_table_exist(table_name, dynamodb_client):
        print("Table already exists.")
        insert_data_into_dynamodb(table_name, result_dict, dynamodb_client,aws_region,aws_access_key_id,aws_secret_access_key)
    else:
        # Create the table if it doesn't exist
        create_dynamodb_table(table_name, partition_key_name, partition_key_type, dynamodb_client)        