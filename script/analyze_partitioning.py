import boto3
import datetime

def get_info(graph, application, index):
    global start_time, client
    resp = client.filter_log_events(
        logGroupName='/aws/lambda/{}_{}_{}'.format(graph, application, index),
        startTime=int(start_time * 1000),
    )
    events = resp['events']
    msg_size_list, overall_time_list, idle_time_list, durations = [], [], [], []
    for event in events:
        message = str(event['message'])
        if 'Duration' in message:
            duration = float(message.split('Duration: ')[1].split(' ms')[0])
            durations.append(duration)
        if 'total_msg_size' in message:
            total_msg_size = float(message.split('total_msg_size: ')[1].split(' MB')[0])
            msg_size_list.append(total_msg_size)
        if 'overall_time' in message:
            overall_time = float(message.split('overall_time: ')[1].split(' s')[0])
            overall_time_list.append(overall_time)
        if 'idle_time' in message:
            idle_time = float(message.split('overall_idle_time: ')[1].split(' s')[0])
            idle_time_list.append(idle_time)
    return durations, msg_size_list, overall_time_list, idle_time_list

if __name__ == '__main__':
    start_time = datetime.datetime(2025, 3, 24, 6, 57, tzinfo=datetime.timezone.utc).timestamp()
    client = boto3.client('logs')
    
    graph_name = 'twitter'
    application = 'pr-checkerboard'

    if graph_name == 'livejournal':
        if application == 'bfs' or application == 'pr':
            cmd_num = 1
        elif application == 'sssp' or application == 'cc':
            cmd_num = 1
    elif graph_name == 'twitter':
        if application == 'bfs' or application == 'pr' or application.startswith('pr'):
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

    all_msg_size_list, all_overall_time_list, all_idle_time_list, all_durations_list = [], [], [], []
    cnt = 0
    for i in range(cmd_num):
        durations, msg_size_list, overall_time_list, idle_time_list = get_info(graph_name, application, i)
        for duration in durations:
            cnt += 1
            print(f'duration = {duration}\n')
        all_msg_size_list.append(msg_size_list)
        all_overall_time_list.append(overall_time_list)
        all_idle_time_list.append(idle_time_list)
        all_durations_list.append(durations)
    print(cnt)

    sum_overall_time, sum_idle_time, sum_msg_size = [], [], []
    max_durations = []
    num_positions = len(all_msg_size_list[0])
    for pos in range(num_positions):
        sum_overall_time.append(sum(all_overall_time[pos] for all_overall_time in all_overall_time_list))
        sum_idle_time.append(sum(all_idle_time[pos] for all_idle_time in all_idle_time_list))
        sum_msg_size.append(sum(all_msg_size[pos] for all_msg_size in all_msg_size_list))
        max_durations.append(max(all_durations[pos] for all_durations in all_durations_list))

    print(f'max_durations = {max_durations}')
    print(f'sum_overall_time = {sum_overall_time}')
    print(f'sum_idle_time = {sum_idle_time}')
    print(f'sum_msg_size = {sum_msg_size}')