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
# USER_NAME = 'api-test-user@test.no'
USER_NAME = 'api-test-user@test.no'
API_ENDPOINT = 'https://api.dev.nva.aws.unit.no/publication'
print(USER_POOL_ID)
print(CLIENT_ID)

def login(username):
    client = boto3.client('cognito-idp')
    password = 'P%' + str(uuid.uuid4())
    response = client.admin_set_user_password(
        Password=password,
        UserPoolId=USER_POOL_ID,
        Username=username,
        Permanent=True,
    )
    print(response)
    response = client.admin_initiate_auth(
        UserPoolId=USER_POOL_ID,
        ClientId=CLIENT_ID,
        AuthFlow='ADMIN_USER_PASSWORD_AUTH',
        AuthParameters={
            'USERNAME': username,
            'PASSWORD': password
        })
    print(response)
    return response['AuthenticationResult']['IdToken']

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
    print(bearer_token)
    with open('registration_titles.json') as registrations_file:
        registrations = json.load(registrations_file)
        for registration in registrations:
            new_registration = create_registration(registration)
            headers = {
                'Authorization': 'Bearer {}'.format(bearer_token),
                'Accept': 'application/json'
            }
            print(headers)
            r = requests.post(url = API_ENDPOINT, json = new_registration, headers=headers)
            print(r.__dict__)

def run():
    bearer_token = login(USER_NAME)
    create_registrations(bearer_token)

if __name__ == '__main__':
    run()