import boto3
import json
import uuid
import copy

dynamodb_client = boto3.client('dynamodb')

RESOURCE_TABLE_NAME = 'nva_resources'
CUSTOMERS_TABLENAME = 'nva_customers'
INSTITUTION_NAME = 'Test Institution'
USER_NAME = 'api-test-user@test.no'


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


def delete_registrations():
    print('Deleting registrations...')
    resources = scan_items(tableName=RESOURCE_TABLE_NAME)
    for resource in resources:
        if resource['type']['S'] == 'Resource':
            primary_partition_key = resource['PK0']['S']
            primary_sort_key = resource['SK0']['S']
            registration = resource['data']['M']
            identifier = registration['identifier']['S']
            owner = registration['owner']['S']
            if USER_NAME in owner:
                print(
                    'Deleting {} - {}'.format(identifier, owner))
                try:
                    response = dynamodb_client.delete_item(
                        TableName=RESOURCE_TABLE_NAME,
                        Key={
                            'PK0': {
                                'S': primary_partition_key
                            },
                            'SK0': {
                                'S': primary_sort_key
                            }
                        })
                except e:
                    print(e)


def find_customer(institutionName):
    customers = scan_items(tableName=CUSTOMERS_TABLENAME)
    for customer in customers:
        if customer['name']['S'] == institutionName:
            return customer['identifier']['S']
    return 'Not found'


def create_pk0(pk0_template, customer, username):
    pk0 = pk0_template.replace('<type>', 'Resource').replace(
        '<customerId>', customer).replace('<userId>', username)
    return pk0


def create_pk1(pk1_template, customer, status):
    pk1 = pk1_template.replace('<type>', 'Resource').replace(
        '<customerId>', customer).replace('<status>', status)
    return pk1


def create_pk2(pk2_template, customer, identifier):
    pk2 = pk2_template.replace('<type>', 'Resource').replace(
        '<customerId>', customer).replace('<resourceId>', identifier)
    return pk2


def create_resource_key(template, identifier):
    key = template.replace('<type>', 'Resource').replace(
        '<resourceId>', identifier)
    return key


def create_resource(resource_template, customer, identifier, username, status):
    new_resource = copy.deepcopy(resource_template)
    new_resource['PK0']['S'] = create_pk0(pk0_template=str(
        new_resource['PK0']['S']), customer=customer, username=username)
    new_resource['PK1']['S'] = create_pk1(pk1_template=str(
        new_resource['PK1']['S']), customer=customer, status=status)
    new_resource['PK2']['S'] = create_pk2(pk2_template=str(
        new_resource['PK2']['S']), customer=customer, identifier=identifier)
    new_resource['PK3']['S'] = create_resource_key(
        template=str(new_resource['PK3']['S']), identifier=identifier)
    new_resource['SK0']['S'] = create_resource_key(
        template=str(new_resource['SK0']['S']), identifier=identifier)
    new_resource['SK1']['S'] = create_resource_key(
        template=str(new_resource['SK1']['S']), identifier=identifier)
    new_resource['SK2']['S'] = create_resource_key(template=str(
        new_resource['SK2']['S']), identifier=identifier).replace('<resourceSort>', 'a')
    new_resource['SK3']['S'] = create_resource_key(
        template=str(new_resource['SK3']['S']), identifier=identifier)
    new_resource['type']['S'] = 'Resource'
    return new_resource

def create_id_entry(identifier):
    with open('id_entry.json') as id_entry_file:
        id_entry_template = json.load(id_entry_file)
        id_entry = copy.deepcopy(id_entry_template)
        id_entry['PK0']['S'] = id_entry['PK0']['S'].replace('<ResourceId>', identifier)
        id_entry['SK0']['S'] = id_entry['SK0']['S'].replace('<ResourceId>', identifier)
        try:
            response = dynamodb_client.put_item(TableName=RESOURCE_TABLE_NAME, Item=id_entry)
        except:
            print('Error creating idEntry for identifier: {}'.format(identifier)) 

def create_registrations():
    print('Creating registrations...')

    customer = find_customer(institutionName=INSTITUTION_NAME)
    print(customer)
    if customer != 'Not found':
        with open('resource.json') as resource_file:
            resource_template = json.load(resource_file)
        with open('test_registrations.json') as registrations_file:
            registrations = json.load(registrations_file)
            for registration in registrations:
                identifier = str(uuid.uuid4())
                resource = create_resource(
                    resource_template=resource_template,
                    customer=customer,
                    identifier=identifier,
                    username=USER_NAME,
                    status='PUBLISHED'
                )
                registration['identifier']['S'] = identifier
                resource['data']['M'] = registration

                print('Created registration with identifier: {}'.format(identifier))
                try:
                    response = dynamodb_client.put_item(TableName=RESOURCE_TABLE_NAME,
                                                        Item=resource)
                except:
                    print('Error creating Resource with identifier: {}'.format(identifier))
                create_id_entry(identifier=identifier)
    else:
        print('Customer not found')


def run():
    delete_registrations()
    create_registrations()


if __name__ == '__main__':
    run()
