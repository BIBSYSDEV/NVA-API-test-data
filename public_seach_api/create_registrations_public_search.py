import boto3
import json
import uuid
import copy
import requests

ssm = boto3.client('ssm')
USER_POOL_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_ID',
                                 WithDecryption=False)['Parameter']['Value']
CLIENT_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_WEB_CLIENT_ID',
                              WithDecryption=False)['Parameter']['Value']
USER_NAME = 'api-test-user@test.no'
API_ENDPOINT = 'https://api.sandbox.nva.aws.unit.no/publication'
RESOURCE_TABLE_NAME = 'nva-resources-nva-publication-api-master-deploy-nva-publication-api'

def login(username):
    client = boto3.client('cognito-idp')
    password = 'P%' + str(uuid.uuid4())
    response = client.admin_set_user_password(
        Password=password,
        UserPoolId=USER_POOL_ID,
        Username=username,
        Permanent=True,
    )
    response = client.admin_initiate_auth(
        UserPoolId=USER_POOL_ID,
        ClientId=CLIENT_ID,
        AuthFlow='ADMIN_USER_PASSWORD_AUTH',
        AuthParameters={
            'USERNAME': username,
            'PASSWORD': password
        })
    return response['AuthenticationResult']['IdToken']

def scan_items(tableName):
    print('scanning items')
    response = dynamodb_client.scan(TableName=tableName)
    scanned_items = response['Items']
    more_items = 'LastEvaluatedKey' in response
    while more_items:
        start_key = response['LastEvaluatedKey']
        response = dynamodb_client.scan(
            TableName=RESOURCE_TABLE_NAME, ExclusiveStartKey=start_key)
        scanned_items.extend(response['Items'])
        more_items = 'LastEvaluatedKey' in response
    return scanned_items

def delete_registrations(bearer_token):
    print('deleting registrations...')
    resources = scan_items(RESOURCE_TABLE_NAME)
    for resource in resources:
        if resource['type']['S'] == 'Resource':
            registration = resource['data']['M']
            identifier = registration['identifier']['S']
            owner = registration['owner']['S']
            if USER_NAME in owner:
                headers = {
                    'Authorization': 'Bearer {}'.format(bearer_token),
                    'Accept': 'application/json'
                }
                delete_url = '{}/{}'.format(API_ENDPOINT, identifier)
                deleteResponse = requests.delete(url = delete_url, headers = headers)

def create_registration(registration):
    print('creating registration...')
    with open('published_registration.json') as registration_template_file:
        print('with title: {}'.format(registration['title']))
        registration_template = json.load(registration_template_file)
        new_registration = copy.deepcopy(registration_template)
        new_registration['entityDescription']['mainTitle'] = registration['title']
        return new_registration

def create_registrations(bearer_token):
    print('creating registrations...')
    with open('registration_titles.json') as registrations_file:
        registrations = json.load(registrations_file)
        for registration in registrations:
            new_registration = create_registration(registration)
            headers = {
                'Authorization': 'Bearer {}'.format(bearer_token),
                'Accept': 'application/json'
            }
            print (headers)
            createResponse = requests.post(url = API_ENDPOINT, json = new_registration, headers = headers)
            print(createResponse.__dict__)
            identifier = createResponse.json()['identifier']
            publish_url = '{}/{}/publish'.format(API_ENDPOINT, identifier)
            requests.put(url = publish_url, headers = headers)

def run():
    bearer_token = login(USER_NAME)
    create_registrations(bearer_token)

if __name__ == '__main__':
    run()