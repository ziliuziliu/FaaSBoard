from gevent import monkey
monkey.patch_all()

import boto3
import json
import logging
import gevent
import time
import random
import numpy
from botocore.exceptions import ClientError
from gevent.lock import BoundedSemaphore

class LambdaWrapper:
    def __init__(self, lambda_client, iam_resource):
        self.lambda_client = lambda_client
        self.iam_resource = iam_resource

    def invoke_function(self, function_name, function_params, get_log=False):
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                Payload=json.dumps(function_params),
                LogType="Tail" if get_log else "None",
            )
        except ClientError:
            logger.exception("couldn't invoke function %s", function_name)
            raise
        return response

logger = logging.getLogger()
logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level = logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

lambda_client = boto3.client("lambda")
lambda_wrapper = LambdaWrapper(lambda_client, None)

# 43s for aws fargate, 1s for local docker container
PROXY_SCALEUP_TIME = 43
EXECUTION_TIME = 12.52
CAPACITY = 4
PROXY_KEEP_ALIVE_TIME = 15 * 60

class Proxy:
    def __init__(self):
        self.proxy_ip = None
        self.initialized_time = time.time()
        self.ready_time = None
        self.queued = []
        self.running = {}
        self.last_used_time = time.time()
        self.status = 'alive'
        self.m = BoundedSemaphore(1)

class Request:
    def __init__(self, request_id):
        self.request_id = request_id
        self.invoke_at = time.time()
        self.finish_at = None
        self.latency = None

proxies = []
available_ip_list = ['172.31.18.188', '172.31.1.244', '172.31.34.82', '172.31.26.47']
global_begin = time.time()

def run_once(request, proxy):
    global global_begin
    # logger.info('request: %s, start invoking', request.request_id)
    events = []
    for i in range(6):
        function_name = 'twitter_bfs_{}'.format(i)
        function_params = {
            'function_name': function_name,
            'graph_dir': 'graph',
            'result_dir': '/tmp',
            'cores': 2,
            'no_pipeline': False,
            'sparse_only': True,
            'dense_only': False,
            'need_global_degree': False,
            'save_mode': 0,
            's3_bucket': 'ziliuziliu',
            'request_id': request.request_id,
            'bfs_root': 0,
            'dynamic_invoke': False,
            'partition_id': i,
            'elastic_proxy': False,
            'proxy_ip': proxy.proxy_ip,
        }
        events.append(gevent.spawn(lambda_wrapper.invoke_function, function_name, function_params))
    gevent.joinall(events)
    request.finish_at = time.time()
    request.latency = request.finish_at - request.invoke_at
    logger.info('request: %s, invoke at: %s, finish at: %s, latency: %s', request.request_id, request.invoke_at - global_begin, request.finish_at - global_begin, request.latency)
    proxy.m.acquire()
    proxy.running.pop(request.request_id)
    proxy.m.release()

ecs_client = boto3.client('ecs')
desired_count = 0

def scale_up(proxy, sleep_time=None):
    logger.info('scaling up 1 proxy')
    if sleep_time is None:
        gevent.sleep(PROXY_SCALEUP_TIME)
    else:
        gevent.sleep(sleep_time)
    proxy.m.acquire()
    proxy.proxy_ip = available_ip_list.pop(0)
    proxy.ready_time = time.time()
    proxy.m.release()
    gevent.spawn(proxy_spin, proxy)

def proxy_spin(proxy):
    logger.info('proxy %s is ready', proxy.proxy_ip)
    while True:
        if time.time() - proxy.last_used_time > PROXY_KEEP_ALIVE_TIME:
            logger.info('proxy %s is idle, scaling down', proxy.proxy_ip)
            proxy.m.acquire()
            proxy.status = 'dead'
            proxy.m.release()
            break
        proxy.m.acquire()
        if len(proxy.running) < CAPACITY and len(proxy.queued) > 0:
            proxy.last_used_time = time.time()
            pop_requests = min(CAPACITY - len(proxy.running), len(proxy.queued))
            for _ in range(pop_requests):
                request = proxy.queued.pop(0)
                proxy.running[request.request_id] = request
                gevent.spawn(run_once, request, proxy)
            proxy.m.release()
        else:
            proxy.m.release()
        gevent.sleep(0.1)

def run_rpm(rpm):
    for _ in range(2):
        new_proxy = Proxy()
        proxies.append(new_proxy)
        gevent.spawn(scale_up, new_proxy, 1)
    logger.info('currently have %s proxies', len(proxies))
    gevent.sleep(10)
    for minute in range(len(rpm)):
        logger.info('minute {} requests {}'.format(minute, rpm[minute]))
        for _ in range(rpm[minute]):
            request_id = random.randint(1, 10000000)
            if len(proxies) == 0:
                logger.info('request %s no proxy, scaling up', request_id)
                new_proxy = Proxy()
                new_proxy.queued.append(Request(request_id))
                proxies.append(new_proxy)
                gevent.spawn(scale_up, new_proxy)
            else:
                min_wait_time, mx_running, selected_proxy = PROXY_SCALEUP_TIME, -1, None
                for proxy in proxies:
                    estimated_wait_time = 0
                    proxy.m.acquire()
                    if proxy.status == 'dead':
                        proxy.m.release()
                        continue
                    if proxy.ready_time is None:
                        estimated_wait_time = PROXY_SCALEUP_TIME - (time.time() - proxy.initialized_time)
                        estimated_wait_time += len(proxy.queued) // CAPACITY * EXECUTION_TIME
                    else:
                        if len(proxy.queued) == 0:
                            if len(proxy.running) == CAPACITY:
                                estimated_wait_time = EXECUTION_TIME
                            else:
                                estimated_wait_time = 0
                        else:
                            estimated_wait_time = (len(proxy.queued) // CAPACITY + 1) * EXECUTION_TIME
                    proxy.m.release()
                    if estimated_wait_time < min_wait_time:
                        min_wait_time = estimated_wait_time
                        mx_running = len(proxy.running)
                        selected_proxy = proxy
                    elif estimated_wait_time == min_wait_time and len(proxy.running) > mx_running:
                        mx_running = len(proxy.running)
                        selected_proxy = proxy
                if selected_proxy is None:
                    logger.info('request %s no proxy, scaling up', request_id)
                    new_proxy = Proxy()
                    new_proxy.queued.append(Request(request_id))
                    proxies.append(new_proxy)
                    gevent.spawn(scale_up, new_proxy)
                else:
                    # logger.info('request %s use proxy %s', request_id, selected_proxy.proxy_ip)
                    selected_proxy.m.acquire()
                    selected_proxy.queued.append(Request(request_id))
                    selected_proxy.m.release()
            gevent.sleep(60 / rpm[minute])
    logger.info('all requests have sent')
    gevent.sleep(600)

def read_rpm(path):
    with open(path, 'r') as f:
        lines = f.readlines()
    rpm = []
    for line in lines:
        line = line.strip()
        rpm.append(int(int(line.split()[-1])/100))
    logger.info('rpms %s', rpm[88:])
    return rpm[88:]

if __name__ == "__main__":
    run_rpm(read_rpm('../build/trace.txt'))