## RUN WITH SUDO!!!

import os
import argparse
import json
import time

TEMP_DIR = '../build/temp'

def build_function(args, index, dockerfile, aws_config):
    print('====== Build For {} {} ======'.format(args.function_name, index))
    print('====== Extracting Zip ======')
    os.system('rm -rf {}'.format(TEMP_DIR))
    os.system('mkdir -p {}'.format(TEMP_DIR))
    os.system('unzip ../build/lambda_{}.zip -d {}'.format(args.application, TEMP_DIR))
    print('====== Building Image ======')
    image_uri = '{}.dkr.ecr.{}.amazonaws.com/lambda-{}'.format(aws_config['account_id'], aws_config['region'], args.function_name)
    os.system('docker rmi $(docker images -q {})'.format(image_uri))
    os.system('docker build --build-arg FUNCTION_PATH=build/temp --build-arg GRAPH_DIR={}/{} -f ../{} -t {}:{} ..'.format(
        args.graph_root_dir, 
        index, 
        dockerfile,
        image_uri,
        index
    ))
    print('====== Upload to ECR ======')
    os.system('aws ecr batch-delete-image --repository-name lambda-{} --image-ids imageTag={} --region {}'.format(args.function_name, index, aws_config['region']))
    time.sleep(10)
    os.system('docker push {}:{}'.format(image_uri, index))
    time.sleep(10)
    print('====== Create Lambda Function ======')
    os.system('aws lambda delete-function --function-name {}_{} --region {}'.format(args.function_name, index, aws_config['region']))
    time.sleep(10)
    os.system('aws lambda create-function --function-name {}_{} --role {} --package-type Image --code ImageUri={}:{} --timeout 300 --memory-size 3538 --vpc-config SubnetIds={},SecurityGroupIds={} --region {}'.format(
        args.function_name,
        index,
        aws_config['role_arn'],
        image_uri,
        index,
        aws_config['vpc_subnet_ids'],
        aws_config['vpc_security_group_ids'],
        aws_config['region']
    ))
    time.sleep(10)
    print('====== Cleaning Cache ======')
    os.system('docker system prune -f')

def main():
    with open('aws-config.json', 'r') as f:
        aws_config = json.load(f)
    parser = argparse.ArgumentParser()
    parser.add_argument('-graph_root_dir', type=str, required=True, help='root directory for graph dataset in csr binary')
    parser.add_argument('-application', type=str, required=True, help='application type (bfs, cc, pr, sssp)')
    parser.add_argument('-function_name', type=str, required=True, help='function name')
    args = parser.parse_args()
    dockerfile = None
    if args.application == 'bfs':
        dockerfile = 'Dockerfile.out'
    elif args.application == 'pr':
        dockerfile = 'Dockerfile.in'
    os.system('aws ecr create-repository --repository-name lambda-{}'.format(args.function_name))
    os.system('aws ecr get-login-password --region {} | docker login --username AWS --password-stdin {}.dkr.ecr.{}.amazonaws.com'.format(aws_config['region'], aws_config['account_id'], aws_config['region']))
    index = 0
    while True:
        if not os.path.exists('../{}/{}'.format(args.graph_root_dir, index)):
            break
        build_function(args, index, dockerfile, aws_config)
        index += 1

if __name__ == '__main__':
    main()
