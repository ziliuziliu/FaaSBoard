import os
import argparse

# object_name = 'fs_test.zip'
# function_name = 'fs_test'
# s3_client.upload_file('../build/{}'.format(object_name), 'ziliuziliu', object_name)
# os.system('aws lambda update-function-code --function-name {} --s3-bucket ziliuziliu --s3-key {}'.format(function_name, object_name))

TEMP_DIR = '/data/temp'
ROLE_ARN = 'arn:aws:iam::602675752686:role/lambda-all-allow'
S3_BUCKET = 'ziliuziliu'
VPC_SUBNET_IDS = 'subnet-01a7209fc67399ade,subnet-01a2568a77d38c598,subnet-05e10c495f9ba6bcf'
VPC_SECURITY_GROUP_IDS = 'sg-07d915f6ecaa671ad'
REGION = 'ap-southeast-1'

def build_function(args, index):
    # cwd = os.getcwd()
    # print('====== Build For {} {} ======'.format(args.function_name, index))
    # print('====== Extracting Zip ======')
    # os.system('rm -rf {}'.format(TEMP_DIR))
    # os.system('mkdir -p {}'.format(TEMP_DIR))
    # os.system('unzip ../build/{}.zip -d {}'.format(args.function_name, TEMP_DIR))
    # print('====== Copying CSR ======')
    # os.system('mkdir -p {}/graph'.format(TEMP_DIR))
    # os.system('cp {}/{}/* {}/graph/'.format(args.graph_root_dir, index, TEMP_DIR))
    # print('====== Build New Zip ======')
    # os.system('cd {}'.format(TEMP_DIR))
    # os.system('zip -r {}_{}.zip *'.format(args.function_name, index))
    # os.system('cd {}'.format(cwd))
    # print('====== Upload to S3 ======')
    # os.system('aws s3 cp {}/{}_{}.zip s3://ziliuziliu/{}_{}.zip'.format(TEMP_DIR, args.function_name, index, args.function_name, index))
    print('====== Create Lambda Function ======')
    cmd = 'aws lambda create-function --function-name {}_{} --role {} --runtime provided.al2023 --timeout 300 --memory-size 3538 --handler lambda_bfs --code S3Bucket={},S3Key={}_{}.zip --vpc-config SubnetIds={},SecurityGroupIds={} --region {}'.format(args.function_name, index, ROLE_ARN, S3_BUCKET, args.function_name, index, VPC_SUBNET_IDS, VPC_SECURITY_GROUP_IDS, REGION)
    print(cmd)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-graph_root_dir', type=str, required=True, help='root directory for graph dataset in csr binary')
    parser.add_argument('-function_name', type=str, required=True, help='function name')
    args = parser.parse_args()
    index = 0
    build_function(args, index)
    # while True:
    #     path = os.path.join(args, str(index))
    #     if not os.path.exists(path):
    #         break
    #     build_function(path, index)

if __name__ == '__main__':
    main()

### TODO: BUILD IMAGE!!!!!!!