import boto3
import json
import uuid

dynamodb_client = boto3.client('dynamodb')

registrations_tablename = 'nva_resources'

def scan_registrations():
    print('scanning registrations')
    response = dynamodb_client.scan(TableName=registrations_tablename)
    scanned_registrations = response['Items']
    more_items = 'LastEvaluatedKey' in response
    while more_items:
        start_key = response['LastEvaluatedKey']
        response = dynamodb_client.scan(TableName=registrations_tablename, ExclusiveStartKey=start_key)
        scanned_registrations.extend(response['Items'])
        more_items = 'LastEvaluatedKey' in response
    return scanned_registrations

def delete_registrations():
    print('Deleting registrations...')
    registrations = scan_registrations()
    for registration in registrations:
        modifiedDate = registration['modifiedDate']['S']
        identifier = registration['identifier']['S']
        owner = registration['owner']['S']
        if('api-test-user@test.no') in owner:
            print(
                'Deleting {} - {}'.format(identifier, owner))
            try:
                response = dynamodb_client.delete_item(
                    TableName=registrations_tablename,
                    Key={
                        'identifier': {
                            'S': identifier
                        },
                        'modifiedDate': {
                            'S': modifiedDate
                        }
                    })
            except e:
                print(e)

def create_registrations():
    print('Creating registrations...')
    with open('test_registrations.json') as registrations_file:
        registrations = json.load(registrations_file)
        for registration in registrations:
            identifier = str(uuid.uuid4())
            registration['identifier']['S'] = identifier
            print('Created registration with identifier: {}'.format(identifier))
            try:
                response = dynamodb_client.put_item(TableName=registrations_tablename,
                                                    Item=registration)
            except e:
                print(e)

def run():
    delete_registrations()
    create_registrations()

if __name__ == '__main__':
    run()