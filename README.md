# AWS DynamoDB replication
Use this script if you need to copy a DynamoDB from an account to another account on AWS.
This also can be used in the same account, you just need to provide the source account the same as the destination account.

# Script config
This script uses a config.ini file that need to be filled with your AWS credentials to make it easier to configure.

**[aws_source]** must be used as source for your AWS Account DynamoDB table.\
**[aws_source]** must be used as target for your AWS Account DynamoDB table.

Fill your credentials parameters as the example below

    [aws_source]
    aws_access_key_id=
    aws_secret_access_key=
    aws_session_token=
    table_name=

    [aws_target]
    aws_access_key_id=
    aws_secret_access_key=
    aws_session_token=
    table_name=
    create_table=true

Inside the copy_tables.py, on main section, you'll find **page_size** parameter. Use this parameter to increase your PutItem speed, just be carefull as higher values might crash the BulkInsert process for larger tables.


This script was build by following a lot of examples found on internet and changed to accomplish this objective.
