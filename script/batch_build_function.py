## RUN WITH SUDO!!!
## like sudo python3 batch_build_function.py -graph_name twitter -graph_root_dir data/twitter -application pr
## 
import os
import argparse
import json
import time
import subprocess

TEMP_DIR = '../build/temp'

os.environ["AWS_PAGER"] = ""

def build_function(args, index, dockerfile, aws_config, application_name, function_name, detailed_dir):
    print('====== Build For {} {} ======'.format(function_name, index))
    print('====== Extracting Zip ======')
    os.system('rm -rf {}'.format(TEMP_DIR))
    os.system('mkdir -p {}'.format(TEMP_DIR))
    os.system('unzip ../build/lambda_{}.zip -d {}'.format(application_name, TEMP_DIR))
    os.system('cp cacert.pem {}'.format(TEMP_DIR))
    print('====== Building Image ======')
    image_uri = '{}.dkr.ecr.{}.amazonaws.com/lambda-{}-{}'.format(aws_config['account_id'], aws_config['region'], args.graph_name, function_name)
    image_id = subprocess.run(["docker", "images", "-q", f'image_uri:{index}'], text=True, capture_output=True, check=False).stdout.strip()
    if image_id:
        print(f"Found image ID: {image_id}")
        subprocess.run(["docker", "rmi", image_id], check=True)
        print(f"Deleted image: {image_uri}")
    else:
        print("Image doesn't exist")
    os.system('docker build --provenance=false --build-arg FUNCTION_PATH=build/temp --build-arg GRAPH_DIR={}/{}/{} -f ../{} -t {}:{} ..'.format(
        args.graph_root_dir, 
        detailed_dir,
        index, 
        dockerfile,
        image_uri,
        index
    ))
    print('====== Upload to ECR ======')
    os.system('aws ecr batch-delete-image --repository-name lambda-{}-{} --image-ids imageTag={} --region {}'.format(args.graph_name, function_name, index, aws_config['region']))
    time.sleep(10)
    os.system('docker push {}:{}'.format(image_uri, index))
    time.sleep(10)
    print('====== Create Lambda Function ======')
    os.system('aws lambda delete-function --function-name {}_{}_{} --region {}'.format(args.graph_name, function_name, index, aws_config['region']))
    time.sleep(10)
    os.system('aws lambda create-function --function-name {}_{}_{} --role {} --package-type Image --code ImageUri={}:{} --timeout 900 --memory-size 3538 --vpc-config SubnetIds={},SecurityGroupIds={} --region {}'.format(
        args.graph_name,
        function_name,
        index,
        aws_config['role_arn'],
        image_uri,
        index,
        aws_config['vpc_subnet_ids'],
        aws_config['vpc_security_group_ids'],
        aws_config['region']
    ))
    time.sleep(10)
    # print('====== Cleaning Cache ======')
    # os.system('docker system prune -f')

def main():
    with open('aws-config.json', 'r') as f:
        aws_config = json.load(f)
    parser = argparse.ArgumentParser()
    parser.add_argument('-graph_name', type=str, required=True, help='livejournal, twitter, friendster, rmat27')
    parser.add_argument('-graph_root_dir', type=str, required=True, help='root directory for graph dataset in csr binary')
    parser.add_argument('-application', type=str, default='', help='application type (bfs, cc, pr, sssp)')
    args = parser.parse_args()
    dockerfile = None
    valid_applications = ['bfs', 'sssp', 'pr', 'cc']
    if args.application and args.application not in valid_applications:
        parser.error('application must be one of bfs, cc, pr, sssp')

    func_names = [args.application] if args.application else valid_applications
    os.system('aws ecr get-login-password --region {} | docker login --username AWS --password-stdin {}.dkr.ecr.{}.amazonaws.com'.format(aws_config['region'], aws_config['account_id'], aws_config['region']))
    for func_name in func_names:
        os.system('aws ecr create-repository --repository-name lambda-{}-{}'.format(args.graph_name ,func_name))
        index = 0
        print(index)
        detailed_dir = ''
        if func_name == 'bfs':
            detailed_dir = 'unweighted'
            dockerfile = 'Dockerfile.out'
        elif func_name == 'sssp':
            detailed_dir = 'weighted'
            dockerfile = 'Dockerfile.out'
        elif func_name == 'pr':
            detailed_dir = 'unweighted'
            dockerfile = 'Dockerfile.in.deg'
        elif func_name == 'cc':
            detailed_dir = 'undirected'
            dockerfile = 'Dockerfile.out'
        while True:
            if not os.path.exists('../{}/{}/{}'.format(args.graph_root_dir, detailed_dir, index)):
                break
            build_function(args, index, dockerfile, aws_config, func_name , func_name, detailed_dir)
            index += 1

if __name__ == '__main__':
    main()
