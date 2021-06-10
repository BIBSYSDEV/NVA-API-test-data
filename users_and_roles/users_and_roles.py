import boto3
import json

users_roles_tablename = 'UsersAndRolesTable'
user_file_name = 'users.json'

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
    scanned_items = scan_dynamo_db(tablename=users_roles_tablename)
    client = boto3.client('dynamodb')
    for item in scanned_items:
        # Deleting test roles
        if 'ROLE' in item['PrimaryKeyHashKey']['S'] and 'Test' in item['PrimaryKeyHashKey']['S']:
            print('Deleting {}'.format(item['PrimaryKeyHashKey']['S']))
            client.delete_item(
                TableName=users_roles_tablename,
                Key={
                    'PrimaryKeyHashKey': { 'S': item['PrimaryKeyHashKey']['S'] },
                    'PrimaryKeyRangeKey': { 'S': item['PrimaryKeyRangeKey']['S'] }
                }
            )
        if 'USER' in item['PrimaryKeyHashKey']['S'] and 'test-user-api' in item['PrimaryKeyHashKey']['S']:
            print('Deleting {}'.format(item['PrimaryKeyHashKey']['S']))
            client.delete_item(
                TableName=users_roles_tablename,
                Key={
                    'PrimaryKeyHashKey': { 'S': item['PrimaryKeyHashKey']['S'] },
                    'PrimaryKeyRangeKey': { 'S': item['PrimaryKeyRangeKey']['S'] }
                }
            )

def create_roles():
    print('Creating roles')
    test_role = {
      "accessRights": {
        "L": [{
            "S": "READ_DOI_REQUEST"
        }]
        },
        "name": {
            "S": "TestExistingRole"
        },
        "PrimaryKeyHashKey": {
            "S": "ROLE#TestExistingRole"
        },
        "PrimaryKeyRangeKey": {
            "S": "ROLE#TestExistingRole"
        },
        "type": {
            "S": "ROLE"
        }
    }
    client = boto3.client('dynamodb')
    response = client.put_item(TableName=users_roles_tablename, Item=test_role)

def create_users():
    print('Creating users')
    client = boto3.client('dynamodb')
    with open(user_file_name) as user_file:
        users = json.load(user_file)
        for user in users:
            response = client.put_item(TableName=users_roles_tablename, Item=user)


def create_test_data():
    print('Creating test data')
    create_roles()
    create_users()

def run():
    clear_test_data()
    create_test_data()

if __name__ == '__main__':
    run()