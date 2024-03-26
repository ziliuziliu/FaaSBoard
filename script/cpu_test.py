import os
import time
import requests
import json
import pandas
import numpy

def cpu_test(memory):
    os.system('aws lambda update-function-configuration --function-name cpu_test --memory-size {}'.format(memory))
    time.sleep(5)
    resp = requests.post('https://ccpdtgbc33oz5q7vn67ciwva2a0ctxig.lambda-url.ap-southeast-1.on.aws/',
                  json={'top': 20000000})
    print(resp.status_code)
    print(resp.content)
    return float(json.loads(resp.content)['interval'])

if __name__ == '__main__':
    mem, interval = [], []
    for i in range(10240, 10240 + 128, 128):
        mem.append(i)
        results = []
        for j in range(3):
            results.append(cpu_test(i))
        interval.append(numpy.average(results))
        pandas.DataFrame({'memory': mem, 'interval': interval}).to_csv('cpu_test.csv')