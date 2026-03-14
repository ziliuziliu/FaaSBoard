import boto3
import datetime

def get_info(graph, application, index):
    lines = []
    with open('../build/{}.txt'.format(index)) as f:
        lines = f.readlines()
    msg_size_list, overall_time_list, idle_time_list, durations = [], [], [], []
    for line in lines:
        if 'from tick read graph' in line:
            read_graph_time = float(line.split('read graph')[1].split('s')[0])
        if 'total_msg_size' in line:
            total_msg_size = float(line.split('total_msg_size: ')[1].split(' MB')[0])
            msg_size_list.append(total_msg_size)
        if 'overall_time' in line:
            overall_time = float(line.split('overall_time: ')[1].split(' s')[0])
            overall_time -= read_graph_time
            overall_time_list.append(overall_time)
            durations.append(overall_time)
        if 'idle_time' in line:
            idle_time = float(line.split('overall_idle_time: ')[1].split(' s')[0])
            idle_time_list.append(idle_time)
    return durations, msg_size_list, overall_time_list, idle_time_list

if __name__ == '__main__':

    graph_name = 'rmat27'
    application = 'pr'

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
        if application == 'bfs' or application == 'pr' or application.startswith('pr'):
            cmd_num = 8
        elif application == 'sssp' or application == 'cc':
            cmd_num = 14
    elif graph_name == 'rmat27':
        if application == 'bfs' or application == 'pr' or application.startswith('pr'):
            cmd_num = 9
        elif application == 'sssp' or application == 'cc':
            cmd_num = 17

    all_msg_size_list, all_overall_time_list, all_idle_time_list, all_durations_list = [], [], [], []
    cnt = 0
    for i in range(cmd_num):
        print(i)
        durations, msg_size_list, overall_time_list, idle_time_list = get_info(graph_name, application, i)
        for duration in durations:
            cnt += 1
            print(f'duration = {duration}')
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