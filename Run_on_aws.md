# Run a Graph Processing Task on AWS

#### AWS Environment Setup

1. **Install AWS CLI**

   AWS CLI installation is already included in `FaaSBoard/script/prepare_machine.sh`.

2. **Configure AWS credentials**

   Run:

   ```bash
   aws configure
   ```

   Example input:

   ```text
   AWS Access Key ID [None]: YOUR_ACCESS_KEY_ID
   AWS Secret Access Key [None]: YOUR_SECRET_ACCESS_KEY
   Default region name [None]: ap-southeast-1
   Default output format [None]: json
   ```

   By default, `aws configure` stores credentials in `~/.aws/`, while `sudo aws configure` typically stores credentials in `/root/.aws/`. If you encounter permission issues when running commands with `sudo`, ensure that credentials are configured for both the current user and the root user as needed.

   To verify the configuration:

   ```bash
   aws sts get-caller-identity
   ```

   A successful response looks similar to:

   ```json
   {
       "UserId": "YOUR_USER_ID",
       "Account": "YOUR_ACCOUNT",
       "Arn": "YOUR_ACCOUNT_ARN"
   }
   ```

   Next, you need to fill the `FaaSBoard/script/aws-config.json` with your aws configuration, here is an example:

   ```json
   {
    "region": "ap-southeast-1",
    "account_id": "123456789123",
    "role_arn": "arn:aws:iam::123456789123:role/lambda-role",
    "vpc_subnet_ids": "subnet-1,subnet-2,subnet-3",
    "vpc_security_group_ids": "sg-0"
   }
   ```

3. **Configure AWS roles and networking**

   Configure the IAM roles, VPC, subnets, and security groups required by AWS Lambda, AWS Elasticache and AWS Fargate.

   This setup depends on your AWS environment. Refer to the official AWS documentation for details:

   - [IAM Roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html?icmpid=docs_iam_help_panel)
   - [VPC](https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html)
   - [VPC and Subnet](https://docs.aws.amazon.com/vpc/latest/userguide/configure-your-vpc.html)
   - [Security Group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html?icmpid=docs_ec2_console#creating-security-group)
   - [Security Group Rules](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/security-group-rules-reference.html?icmpid=docs_ec2_console)
   - [Managing clusters in Elasticache](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/Clusters.html?icmpid=docs_console_unmapped)

   Correct networking and permission settings are required for communication between AWS Lambda functions, AWS Elasticache and AWS Fargate tasks.

4. **Build and deploy the proxy service on AWS Fargate**

   First, build the `proxy_server` image and push it to AWS ECR. Make sure that AWS CLI and Docker are installed. The proxy is built on `FaaSBoard/Dockerfile`.

   ```bash
   cd script
   sudo ./build_proxy.sh <REPO_NAME>
       -- REPO_NAME: AWS image repository name
   ```

   Then create an AWS Fargate service using the generated proxy image. We recommend using the AWS ECS console for this step.

   Some suggested task definition settings:

   - launch type: AWS Fargate
   - operating system / architecture: Linux / X86_64
   - compute: 16 vCPUs and 64 GB memory
   - image: the ECR image URL generated in the previous step

   After that, create an ECS cluster and deploy a Fargate service in that cluster using the task definition for `proxy_server`.

5. **Build and deploy AWS Lambda functions for graph processing**

   Package the compiled graph processing code together with graph data into images and push them to AWS ECR. Make sure that recent versions of AWS CLI and Docker are installed. 

   ```bash
   cd script
   sudo python3 batch_build_function.py <GRAPH_NAME> <GRAPH_ROOT_DIR> <APPLICATION>
       -- GRAPH_NAME: graph dataset name (livejournal, twitter, friendster, rmat27)
       -- GRAPH_ROOT_DIR: root directory of the graph dataset in CSR binary format
       -- APPLICATION: graph application (for example, bfs)
   # example
   sudo python3 batch_build_function.py -graph_name twitter -graph_root_dir data/twitter --application bfs
   ```

6. **Configure AWS ElastiCache**

   AWS ElastiCache stores the list of AWS Fargate IP addresses. In this project, the list is named `GlobalIPList`. AWS Lambda functions read this list to locate Fargate proxy endpoints.

   First, create a Valkey cache in AWS ElastiCache. Then create the `GlobalIPList` list object and populate it with the IP addresses of the running Fargate tasks.

   Option 1: connect to the cache through AWS CloudShell and run a command such as:

   ```bash
   # example
   LPUSH GlobalIPList "172.16.1.1" "172.16.1.2" "172.16.1.3" "172.16.1.4"
   ```

   Option 2: package `FaaSBoard/script/update-fargate-endpoints.py` as an image, push it to AWS ECR, deploy it as an AWS Lambda function, and invoke it to create or update the Fargate endpoint list.

#### Run the Graph Processing Task

Start the configured AWS Fargate service and update the corresponding IP addresses in ElastiCache.

Then launch the graph processing task. The following example runs `livejournal bfs` with 4 partitions:

```bash
aws lambda invoke --function-name livejournal_bfs_0 --cli-binary-format raw-in-base64-out --payload '{"function_name": "livejournal_bfs_0", "bfs_root": 0, "graph_dir": "graph", "result_dir": "/tmp", "cores": 2, "no_pipeline": false, "out_csr": false, "in_csr": false, "need_global_degree": false, "save_mode": 0, "s3_bucket": "your_s3_bucket", "request_id": 101, "dynamic_invoke": false, "partition_id": 0, "elastic_proxy": true, "elasticache_host": "your-elasticache-host"}' result0.txt &
aws lambda invoke --function-name livejournal_bfs_1 --cli-binary-format raw-in-base64-out --payload '{"function_name": "livejournal_bfs_1", "bfs_root": 0, "graph_dir": "graph", "result_dir": "/tmp", "cores": 2, "no_pipeline": false, "out_csr": false, "in_csr": false, "need_global_degree": false, "save_mode": 0, "s3_bucket": "your_s3_bucket", "request_id": 101, "dynamic_invoke": false, "partition_id": 1, "elastic_proxy": true, "elasticache_host": "your-elasticache-host"}' result1.txt &
aws lambda invoke --function-name livejournal_bfs_2 --cli-binary-format raw-in-base64-out --payload '{"function_name": "livejournal_bfs_2", "bfs_root": 0, "graph_dir": "graph", "result_dir": "/tmp", "cores": 2, "no_pipeline": false, "out_csr": false, "in_csr": false, "need_global_degree": false, "save_mode": 0, "s3_bucket": "your_s3_bucket", "request_id": 101, "dynamic_invoke": false, "partition_id": 2, "elastic_proxy": true, "elasticache_host": "your-elasticache-host"}' result2.txt &
aws lambda invoke --function-name livejournal_bfs_3 --cli-binary-format raw-in-base64-out --payload '{"function_name": "livejournal_bfs_3", "bfs_root": 0, "graph_dir": "graph", "result_dir": "/tmp", "cores": 2, "no_pipeline": false, "out_csr": false, "in_csr": false, "need_global_degree": false, "save_mode": 0, "s3_bucket": "your_s3_bucket", "request_id": 101, "dynamic_invoke": false, "partition_id": 3, "elastic_proxy": true, "elasticache_host": "your-elasticache-host"}' result3.txt &
```

Common payload fields:

- `function_name`: Lambda function name, usually identical to the deployed function name.
- `request_id`: Request identifier for the current query.
- `partition_id`: Partition identifier handled by the current Lambda function.
- `graph_dir`: Directory of graph data inside the container image.
- `result_dir`: Output directory for results. Lambda functions typically use `/tmp`.
- `cores`: Number of logical CPU cores assigned to the function.
- `no_pipeline`: Disable pipeline mode when set to `true`.
- `out_csr`: Enable out-CSR mode (bfs, sssp, cc).
- `in_csr`: Enable in-CSR mode (pr).
- `need_global_degree`: Load global degree information when required.
- `save_mode`: Result save mode (`0`: no save, `1`: local disk, `2`: S3).
- `s3_bucket`: S3 bucket used when `save_mode=2`.
- `dynamic_invoke`: Enable Lambda-to-Lambda reinvocation.
- `elastic_proxy`: Retrieve proxy endpoints from ElastiCache.
- `elasticache_host`: ElastiCache endpoint.
- `proxy_ip`: Directly specify the proxy IP when `elastic_proxy` is disabled.
- `kill_wait_ms`: Wait time before proactive termination. Default: `100000`.
- `pair_sparse_boundary`: Sparse boundary threshold. Default: `100000`.
- `sparse_dense_boundary`: Dense boundary threshold. Default: `1000000`.

Application-specific fields:

- `bfs`: `bfs_root`
- `pr`: `pr_iterations`
- `sssp`: `sssp_root`
- `cc`: no additional required fields

You can see these in `FaaSBoard/script/run.sh` and `FaaSBoard/include/util/flags.h`.
