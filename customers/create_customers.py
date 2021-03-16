import boto3
import uuid
import json

customer_tablename = 'nva_customers'
customer_file_name = 'test_customers.json'

def scan_dynamo_db(tablename):
    client = boto3.client('dynamodb')
    response = client.scan(TableName=tablename)
    scanned_items = response['Items']
    more_items = 'LastEvaluatedKey' in response
    while more_items:
        start_key = response['LastEvaluatedKey']
        response = client.scan(TableName=tablename, ExclusiveStartKey=start_key)
        scanned_items.extend(response['Items'])
        more_items = 'LastEvaluatedKey' in response
    return scanned_items

def clear_test_data():
    print('clear test data')
    scanned_customers = scan_dynamo_db(tablename=customer_tablename)
    client = boto3.client('dynamodb')
    for customer in scanned_customers:
        if 'archiveName' in customer and 'API Test Archive' == customer['archiveName']['S']:
            print('Deleting {}'.format(customer['name']['S']))
            client.delete_item(
                TableName=customer_tablename,
                Key={
                    'identifier': { 'S': customer['identifier']['S'] }
                }
            )

def create_test_data():
    print('create test data')
    client = boto3.client('dynamodb')
    with open(customer_file_name) as customer_file:
        customers = json.load(customer_file)
        for customer in customers:
            identifier = str(uuid.uuid4())
            customer['identifier']['S'] = identifier
            response = client.put_item(TableName=customer_tablename, Item=customer)


def run():
    clear_test_data()
    create_test_data()

if __name__ == '__main__':
    run()