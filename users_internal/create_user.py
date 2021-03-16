import boto3
import json

user_tablename = 'UsersAndRolesTable'
customer_tablename = 'nva_customers'
customers = []
user_file_name = 'test_users.json'

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

def read_customers():
    customers.extend(scan_dynamo_db(tablename=customer_tablename))

def clear_test_data():
    print('clear test data')
    scanned_users = scan_dynamo_db(tablename=user_tablename)
    client = boto3.client('dynamodb')
    for user in scanned_users:
        if 'givenName' in user and 'User internal' in user['givenName']['S']:
            print('Deleting {}'.format(user['username']['S']))
            client.delete_item(
                TableName=user_tablename,
                Key={
                    'PrimaryKeyHashKey': { 'S': user['PrimaryKeyHashKey']['S'] },
                    'PrimaryKeyRangeKey': { 'S': user['PrimaryKeyRangeKey']['S'] }
                }
            )

def find_customer(customer_id):
    for customer in customers:
        if customer['feideOrganizationId']['S'] == customer_id:
            return customer
    print('Could not find customer with id {}'.format(customer_id))

def create_test_data():
    print('create test data')
    client = boto3.client('dynamodb')
    with open(user_file_name) as user_file:
        users = json.load(user_file)
        for user in users:
            customer = find_customer(user['institution']['S'])
            user['institution']['S'] = 'https://api.dev.nva.aws.unit.no/customer/{}'.format(customer['identifier']['S'])
            response = client.put_item(TableName=user_tablename, Item=user)


def run():
    read_customers()
    clear_test_data()
    create_test_data()

if __name__ == '__main__':
    run()
