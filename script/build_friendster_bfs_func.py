## RUN WITH SUDO!!!
## like sudo python3 batch_build_function.py -graph_name friendster -graph_root_dir ../data/friendster
## 
import os
import argparse
import json
import time
import subprocess

TEMP_DIR = '../build/temp'

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
    os.system('docker build --build-arg FUNCTION_PATH=build/temp --build-arg GRAPH_DIR={}/{}/{} -f ../{} -t {}:{} ..'.format(
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
    lambda_function_name = 'cache_bfs_{}'.format(index)  # 修改为新的函数命名格式
    # 检查函数是否存在，如果存在则删除
    delete_result = os.system('aws lambda delete-function --function-name {} --region {}'.format(lambda_function_name, aws_config['region']))
    if delete_result != 0:
        print(f"Function {lambda_function_name} may not exist or could not be deleted. Continuing...")
    time.sleep(10)
    
    create_command = 'aws lambda create-function --function-name {} --role {} --package-type Image --code ImageUri={}:{} --timeout 900 --memory-size 3538 --vpc-config SubnetIds={},SecurityGroupIds={} --region {}'.format(
        lambda_function_name,
        aws_config['role_arn'],
        image_uri,
        index,
        aws_config['vpc_subnet_ids'],
        aws_config['vpc_security_group_ids'],
        aws_config['region']
    )
    print(f"Running command: {create_command}")
    create_result = os.system(create_command)
    if create_result != 0:
        print(f"Error creating Lambda function {lambda_function_name}. Exit code: {create_result}")
    else:
        print(f"Successfully created Lambda function {lambda_function_name}")
    time.sleep(10)
    # print('====== Cleaning Cache ======')
    # os.system('docker system prune -f')

def main():
    with open('aws-config.json', 'r') as f:
        aws_config = json.load(f)
    parser = argparse.ArgumentParser()
    parser.add_argument('-graph_name', type=str, default='friendster', help='livejournal, twitter, friendster, rmat27')
    parser.add_argument('-graph_root_dir', type=str, default='../data/friendster', help='root directory for graph dataset in csr binary')
    args = parser.parse_args()
    
    # 使用命令行参数或默认值
    if args.graph_name == '':
        args.graph_name = 'friendster'
    if args.graph_root_dir == '':
        args.graph_root_dir = '../data/friendster'
        
    dockerfile = None
    os.system('aws ecr get-login-password --region {} | docker login --username AWS --password-stdin {}.dkr.ecr.{}.amazonaws.com'.format(aws_config['region'], aws_config['account_id'], aws_config['region']))
    
    func_name = 'bfs'
    detailed_dir = 'unweighted'
    dockerfile = 'Dockerfile.out'
    
    # 尝试创建ECR仓库，但忽略已存在错误
    create_repo_command = 'aws ecr create-repository --repository-name lambda-{}-{} 2>/dev/null || echo "Repository already exists"'.format(args.graph_name, func_name)
    os.system(create_repo_command)
    
    # 只运行0到7的索引
    for index in range(8):  # 0~7
        path = '../{}/{}/{}'.format(args.graph_root_dir, detailed_dir, index)
        if not os.path.exists(path):
            print(f"Warning: Path '{path}' does not exist. Creating empty directory for testing.")
            os.makedirs(path, exist_ok=True)
        
        build_function(args, index, dockerfile, aws_config, func_name, func_name, detailed_dir)
        print(f"Completed processing for index {index}")

if __name__ == '__main__':
    main()