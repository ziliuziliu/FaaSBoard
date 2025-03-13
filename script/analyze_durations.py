import boto3
import datetime

def get_durations(graph, application, index):
    global start_time, client
    resp = client.filter_log_events(
        logGroupName='/aws/lambda/{}_{}_{}'.format(graph, application, index),
        startTime=int(start_time * 1000),
    )
    events = resp['events']
    durations = []
    for event in events:
        message = str(event['message'])
        if 'Duration' in message:
            duration = float(message.split('Duration: ')[1].split(' ms')[0])
            durations.append(duration)
    return durations

if __name__ == '__main__':
    start_time = datetime.datetime(2025, 3, 13, 6, 40, tzinfo=datetime.timezone.utc).timestamp()
    client = boto3.client('logs')
    
    all_durations = []
    
    graph_name = 'twitter'
    application = 'cc'

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

    for i in range(cmd_num):
        durations = get_durations(graph_name, application, i)
        all_durations.append(durations)
    
    max_durations = []
    if all_durations:
        num_positions = len(all_durations[0])
        for pos in range(num_positions):
            max_value = max(durations[pos] for durations in all_durations)
            max_durations.append(max_value)
    
    print(f"Maximum durations at each position: {max_durations}")