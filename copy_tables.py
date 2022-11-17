import sys
from time import sleep
import boto3
import os
import configparser
import itertools

def create_table(src_table, dst_table, src_dynamo, dst_dynamo):
    # get source table and its schema
    print("Describe table '" + src_table + "'")
    try:
        table_schema = src_dynamo.describe_table(TableName=src_table)["Table"]
    except src_dynamo.exceptions.ResourceNotFoundException:
        print("!!! Table {0} does not exist. Exiting...".format(src_table))
        sys.exit(1)
    except:
        print("!!! Error reading table {0} . Exiting...".format(src_table))
        sys.exit(1)

    print("*** Reading key schema from {0} table".format(src_table))

    # create keyword args for copy table
    keyword_args = {"TableName": dst_table}

    keyword_args['KeySchema'] = table_schema['KeySchema']
    keyword_args['AttributeDefinitions'] = table_schema['AttributeDefinitions']

    global_secondary_indexes = []
    local_secondary_indexes = []

    if table_schema.get("GlobalSecondaryIndexes"):
        for item in table_schema["GlobalSecondaryIndexes"]:
            index = {}
            for k, v in item.items():
                if k in ["IndexName", "KeySchema", "Projection", "ProvisionedThroughput"]:
                    if k == "ProvisionedThroughput":
                        # uncomment below to have same read/write capacity as original table
                        # for key in v.keys():
                        #     if key not in ["ReadCapacityUnits", "WriteCapacityUnits"]:
                        #         del v[key]

                        # comment below to have same read/write capacity as original table
                        index[k] = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
                        continue
                    index[k] = v
            global_secondary_indexes.append(index)

    if table_schema.get("LocalSecondaryIndexes"):
        for item in table_schema["LocalSecondaryIndexes"]:
            index = {}
            for k, v in item.iteritems():
                if k in ["IndexName", "KeySchema", "Projection"]:
                    index[k] = v
            local_secondary_indexes.append(index)

    if global_secondary_indexes:
        keyword_args["GlobalSecondaryIndexes"] = global_secondary_indexes
    if local_secondary_indexes:
        keyword_args["LocalSecondaryIndexes"] = local_secondary_indexes

    # uncomment below to have same read/write capacity as original table
    # provisionedThroughput = table_schema['ProvisionedThroughput']
    # for key in provisionedThroughput.keys():
    #     if key not in ["ReadCapacityUnits", "WriteCapacityUnits"]:
    #         del provisionedThroughput[key]

    # keyword_args["ProvisionedThroughput"] = provisionedThroughput

    # comment below to have same read/write capacity as original table
    keyword_args["ProvisionedThroughput"] = {"ReadCapacityUnits": 3, "WriteCapacityUnits": 1200}

    if table_schema.get('StreamSpecification'):
        keyword_args['StreamSpecification'] = table_schema['StreamSpecification']

    # create copy table
    try:
        table = dst_dynamo.describe_table(TableName=dst_table)
        print("!!! Table {0} already exists. Exiting...".format(dst_table))
        
    except dst_dynamo.exceptions.ResourceNotFoundException:
        dst_dynamo.create_table(**keyword_args)

        print("*** Waiting for the new table {0} to become active".format(dst_table))
        sleep(5)

        while dst_dynamo.describe_table(TableName=dst_table)['Table']['TableStatus'] != 'ACTIVE':
            sys.stdout.write(next(spinner))
            sys.stdout.flush()
            sleep(0.1)
            sys.stdout.write('\b')
        print("*** New table {0} to is now active!".format(dst_table))
    
def copyTable(sourceTable, destinationTable, src_dynamo, dst_dynamo, page_size):
    print("Inside copyTable")
    print("Coping", sourceTable, "to", destinationTable)

    item_count = 0
    dynamopaginator = src_dynamo.get_paginator('scan')

    print('Start Reading the Source Table')
    try:
            dynamoresponse = dynamopaginator.paginate(
            TableName=sourceTable,
            Select='ALL_ATTRIBUTES',
            ReturnConsumedCapacity='NONE',
            ConsistentRead=True,
            PaginationConfig={"PageSize": page_size}
        )
    except src_dynamo.exceptions.ResourceNotFoundException:
        print("Table does not exist")
        print("Exiting")
        sys.exit()

    print('Finished Reading the Table')
    print('Proceed with writing to the Destination Table')
    print(dynamoresponse)
    
    for page in dynamoresponse:
        batch = []
        for item in page['Items']:
            item_count += 1
            batch.append({
                'PutRequest': {
                    'Item': item
                }
            })
        
        try:
            print("Process put {0} items".format(item_count))
            dst_dynamo.batch_write_item(
                RequestItems={
                dst_table: batch
                }
            )
        except:
            print("Error while copying sequence {0}".format(item_count))
            pass;




if __name__ == "__main__":
    spinner     = itertools.cycle(['-', '/', '|', '\\'])
    file_path   = os.path.dirname(sys.argv[0]) + '/'
    page_size   = 10  # if too large, can cause batch put issues
    counter     = 1
    
    if(os.path.isfile(file_path + 'config.ini')==False):
        print('Config not found')
        exit(1)

    config = configparser.ConfigParser()
    config.read(file_path + 'config.ini')

    src_dynamo = boto3.client('dynamodb', region_name='us-east-1',
        aws_access_key_id=config.get('aws_source', 'aws_access_key_id'),
        aws_secret_access_key=config.get('aws_source', 'aws_secret_access_key'),
        aws_session_token=config.get('aws_source', 'aws_session_token'))

    dst_dynamo = boto3.client('dynamodb', region_name='us-east-1',
        aws_access_key_id=config.get('aws_target', 'aws_access_key_id'),
        aws_secret_access_key=config.get('aws_target', 'aws_secret_access_key'),
        aws_session_token=config.get('aws_target', 'aws_session_token'))

    src_table=config.get('aws_source', 'table_name')
    dst_table=config.get('aws_target', 'table_name')

    print("AWS Source ID {0}".format(config.get('aws_source', 'aws_access_key_id')))
    print("AWS Destination ID {0}".format(config.get('aws_target', 'aws_access_key_id')))
    print("Are you sure you want to copy table source {0} to table destination {1} ?".format(src_table, dst_table))
    if(input("yes/no: ")=="yes"):

        if(config.get('aws_target', 'create_table')=='true'):
            # Create the new table
            create_table(src_table, dst_table, src_dynamo, dst_dynamo)

        # Copy data to new table
        copyTable(src_table, dst_table, src_dynamo, dst_dynamo, page_size)
    else:
        print("Ok, exiting")