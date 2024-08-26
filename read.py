's3://kinesis-rtfd/data/customer_id=104/kinesis-rtfd-1-2024-04-13-19-11-31-0adae19a-72c0-3dd8-bd0e-76e80f20831a'

import boto3 

client = boto3.client('s3')

path = 's3://kinesis-rtfd/data/'

for folders in path:
    print(folders)