import boto3
import datetime

def get_billed_duration(index):
    global start_time, client
    resp = client.filter_log_events(
        logGroupName='/aws/lambda/pr_e_{}'.format(index),
        startTime=int(start_time * 1000),
    )
    events = resp['events']
    billed_duration = 0
    for event in events:
        message = str(event['message'])
        if 'Billed Duration' in message:
            billed_duration += int(message.split('Billed Duration: ')[1].split(' ms')[0])
    print('billed duration for {}: {}'.format(index, billed_duration))
    return billed_duration

if __name__ == '__main__':
    start_time = datetime.datetime(2025, 2, 5, 16, 8, tzinfo=datetime.timezone.utc).timestamp()
    client = boto3.client('logs')
    total_billed_duration = 0
    for i in range(8):
        total_billed_duration += get_billed_duration(i)
    print('total billed duration: ', total_billed_duration)