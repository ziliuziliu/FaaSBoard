import boto3
import datetime

def get_duration_memory(graph, application, index):
    global start_time, client
    resp = client.filter_log_events(
        logGroupName='/aws/lambda/{}_{}_{}'.format(graph, application, index),
        startTime=int(start_time * 1000),
    )
    events = resp['events']
    durations, memories = [], []
    cnt = 0
    for event in events:
        message = str(event['message'])
        if 'Duration' in message:
            duration = float(message.split('Duration: ')[1].split(' ms')[0])
            memory = float(message.split('Max Memory Used: ')[1].split(' MB')[0])
            cnt += 1
            durations.append(duration)
            memories.append(memory)
    return durations, memories

if __name__ == '__main__':
    start_time = datetime.datetime(2025, 4, 12, 7, 33, tzinfo=datetime.timezone.utc).timestamp()
    client = boto3.client('logs')
    
    all_durations, all_memories = [], []
    
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

    cnt = 0
    for i in range(cmd_num):
        durations, memories = get_duration_memory(graph_name, application, i)
        for duration in durations:
            cnt += 1
            print(f'duration = {duration}\n')
        all_durations.append(durations)
        all_memories.append(memories)
    
    print(cnt)
    print(all_memories)

    max_durations, sum_memories = [], []
    if all_durations:
        num_positions = len(all_durations[0])
        for pos in range(num_positions):
            max_duration = max(durations[pos] for durations in all_durations)
            sum_memory = sum(memories[pos] for memories in all_memories)
            max_durations.append(max_duration)
            sum_memories.append(sum_memory)
    
    print(f"Maximum durations at each position: {max_durations}")
    print(f"Sum of memories at each position: {sum_memories}")