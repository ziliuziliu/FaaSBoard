import json

if __name__ == '__main__':

    graph_name = 'twitter'
    application = 'pr'
    path = '/home/ubuntu/FaaSBoard/data/twitter/unweighted-accurate'

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

    mx_workload, sum_workload = 0, 0
    for i in range(cmd_num):
        with open('{}/{}/graphs.meta'.format(path, i)) as f:
            meta = json.load(f)
            workload = 0
            for graph in meta['graphs']:
                workload += graph['edges']
                # workload += graph['edges'] + 10 * (graph['to_source'] - graph['from_source'] + graph['to_dest'] - graph['from_dest'])
            print('graph: {}, workload: {}'.format(i, workload))
            sum_workload += workload
            mx_workload = max(mx_workload, workload)
    print('max workload: {}, sum workload: {}, ratio: {}'.format(mx_workload, sum_workload, mx_workload / (sum_workload / cmd_num)))