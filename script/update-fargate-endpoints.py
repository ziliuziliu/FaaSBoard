import json
import boto3
import redis

def get_task_ips(cluster_name, service_name):
    ecs_client = boto3.client('ecs')
    list_tasks_response = ecs_client.list_tasks(
        cluster=cluster_name,
        serviceName=service_name,
        desiredStatus='RUNNING'
    )
    task_arns = list_tasks_response['taskArns']
    if not task_arns:
        print(f"No running tasks found for service {service_name} in cluster {cluster_name}")
        return []
    describe_tasks_response = ecs_client.describe_tasks(
        cluster=cluster_name,
        tasks=task_arns
    )
    private_ips = []
    for task in describe_tasks_response['tasks']:
        if task['lastStatus'] == 'RUNNING':
            for attachment in task['attachments']:
                if attachment['type'] == 'ElasticNetworkInterface':
                    for detail in attachment['details']:
                        if detail['name'] == 'privateIPv4Address':
                            private_ips.append(detail['value'])
    return private_ips

def set_list(key, values):
    redis_client = redis.StrictRedis(
        host='faasboard-hcdnu5.serverless.apse1.cache.amazonaws.com',  # you need to update this with your own Elasticache host if using elastic proxy
        port=6379, 
        decode_responses=True,
        ssl=True,
        ssl_cert_reqs="none",
    )
    redis_client.delete(key)
    for value in values:
        redis_client.rpush(key, value)

def lambda_handler(event, context):
    private_ips = get_task_ips('faasboard', 'proxy_server') # faasboard is the cluster name of AWS Fargate, proxy_server is the service name of Fargate
    print('fargate ips: {}'.format(private_ips))
    set_list('GlobalIPList', private_ips) # GlobalIPList is the key in Redis to store the list of Fargate IPs
    print('push to elasticache success')
    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }
