import boto3
import json
from decimal import Decimal
from botocore.exceptions import ClientError

def batch_write_with_backoff(client, request_params):
    max_retries = 5
    base_delay = 0.1  # Initial delay in seconds

    for i in range(max_retries):
        try:
            response = client.batch_write_item(**request_params)
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


def create_dynamodb_table(dynamodb_table_name,aws_access_key_id,aws_secret_access_key,aws_region):
    # Specify your AWS region
    aws_region = aws_region

    # Specify your DynamoDB table name
    table_name = dynamodb_table_name

    # Specify the primary key attribute (partition key)
    partition_key_name = 'DocumentId'  # Replace with your desired partition key attribute

    # Specify the attribute types
    partition_key_type = 'S'  # 'S' for string, 'N' for number, etc.

    # Create a DynamoDB client
    dynamodb = boto3.client('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,region_name=aws_region)

    # Define the KeySchema, AttributeDefinitions, and ProvisionedThroughput
    key_schema = [
        {'AttributeName': partition_key_name, 'KeyType': 'HASH'}  # Partition Key
    ]

    attribute_definitions = [
        {'AttributeName': partition_key_name, 'AttributeType': partition_key_type}
    ]

    provisioned_throughput = {
        'ReadCapacityUnits': 5,   # Adjust based on your read requirements
        'WriteCapacityUnits': 5   # Adjust based on your write requirements
    }

    # Create the DynamoDB table
    response = dynamodb.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput=provisioned_throughput
    )

    print("Table creation response:", response)



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

    # Specify your DynamoDB table name
    table_name = table_name

    # Specify the primary key attribute (partition key)
    partition_key_name = 'DocumentId'  # Replace with your desired partition key attribute

    # Specify the attribute types
    partition_key_type = 'S'  # 'S' for string, 'N' for number, etc.

    # Define the KeySchema, AttributeDefinitions, and ProvisionedThroughput
    key_schema = [
        {'AttributeName': partition_key_name, 'KeyType': 'HASH'}  # Partition Key
    ]

    attribute_definitions = [
        {'AttributeName': partition_key_name, 'AttributeType': partition_key_type}
    ]

    provisioned_throughput = {
        'ReadCapacityUnits': 5,   # Adjust based on your read requirements
        'WriteCapacityUnits': 5   # Adjust based on your write requirements
    }

    # Create the DynamoDB table
    response = dynamodb.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput=provisioned_throughput
    )

    print("Table creation response:", response)

    # Specify the DynamoDB table
    #table = dynamodb.Table(table_name)

    # Iterate through items in the data dictionary and put them into DynamoDB
    #with table.batch_writer() as batch:
    with response.batch_writer() as batch:
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

    dynamodb_table_name = 'dataextraction'

    #create_dynamodb_table(dynamodb_table_name,aws_access_key_id,aws_secret_access_key,aws_region)

    # Load s3 json file data into Dynamodb
    load_data_into_dynamodb(result_dict, dynamodb_table_name, aws_access_key_id,aws_secret_access_key,aws_region)