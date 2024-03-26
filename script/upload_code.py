import boto3
import os

object_name = 'cpu_test.zip'
function_name = 'cpu_test'
s3_client = boto3.client('s3')
s3_client.upload_file('../build/{}'.format(object_name), 'ziliuziliu', object_name)
os.system('aws lambda update-function-code --function-name {} --s3-bucket ziliuziliu --s3-key {}'.format(function_name, object_name))