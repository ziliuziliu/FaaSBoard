import boto3
import datetime

def get_billed_duration(graph_name, application, index):
    global start_time, client
    resp = client.filter_log_events(
        logGroupName='/aws/lambda/{}_{}_{}'.format(graph_name, application, index),
        startTime=int(start_time * 1000),
    )
    events = resp['events']
    duration, billed_duration = 0, 0
    print('index = {}'.format(index))
    for event in events:
        message = str(event['message'])
        if 'Billed Duration' in message:
            current_billed_duration = float(message.split('Billed Duration: ')[1].split(' ms')[0])
            current_duration = float(message.split('Duration: ')[1].split(' ms')[0])
            print('duration = {}, billed_duration = {}'.format(current_duration, current_billed_duration))
            billed_duration += current_billed_duration
            duration = current_duration
    return duration, billed_duration

if __name__ == '__main__':
    graph_name = 'twitter'
    application = 'sssp'
    if graph_name == 'livejournal':
        if application == 'bfs' or application == 'pr':
            cmd_num = 1
        elif application == 'sssp' or application == 'cc':
            cmd_num = 1
    elif graph_name == 'twitter':
        if application == 'bfs' or application == 'pr':
            cmd_num = 6
        elif application == 'sssp' or application == 'cc':
            cmd_num = 12
    elif graph_name == 'friendster':
        if application == 'bfs' or application == 'pr':
            cmd_num = 8
        elif application == 'sssp' or application == 'cc':
            cmd_num = 14
    elif graph_name == 'rmat27':
        if application == 'bfs' or application == 'pr':
            cmd_num = 9
        elif application == 'sssp' or application == 'cc':
            cmd_num = 17
    start_time = datetime.datetime(2025, 3, 22, 9, 22, tzinfo=datetime.timezone.utc).timestamp()
    client = boto3.client('logs')
    all_duration, all_billed_duration = 0, 0
    for i in range(cmd_num):
        duration, billed_duration = get_billed_duration(graph_name, application, i)
        all_duration = max(all_duration, duration)
        all_billed_duration += billed_duration
    print('duration: ', all_duration)
    print('total billed duration: ', all_billed_duration)