import sys
import re

def extract(line):
    begin, end, cnt = -1, -1, 0
    for i in range(len(line) - 1, -1, -1):
        if line[i] == 's':
            end = i
        if line[i] == ' ':
            cnt += 1
            if cnt == 2:
                begin = i + 1
                break
    return line[begin : end]

def analyze(log_file_name):
    read_graph, connect, begin, vote, inin, exec_each, outout, exec_diagonal, disconnect, save_result = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    overall = 0
    with open(log_file_name) as f:
        lines = f.readlines()
        for line in lines:
            if line.find('from tick read graph') != -1:
                read_graph += float(extract(line))
            if line.find('from tick connect') != -1:
                connect += float(extract(line))
            if line.find('from tick begin') != -1:
                begin += float(extract(line))
            if line.find('from tick round') != -1 and line.find('vote') != -1:
                vote += float(extract(line))
            if line.find('from tick round') != -1 and line.find('in') != -1:
                inin += float(extract(line))
            if line.find('from tick round') != -1 and line.find('exec_each') != -1:
                exec_each += float(extract(line))
            if line.find('from tick round') != -1 and line.find('out') != -1:
                outout += float(extract(line))
            if line.find('from tick round') != -1 and line.find('exec_diagonal') != -1:
                exec_diagonal += float(extract(line))
            if line.find('from tick disconnect') != -1:
                disconnect += float(extract(line))
            if line.find('from tick save_result') != -1:
                save_result += float(extract(line))
            if line.find('from start overall') != -1:
                overall += float(extract(line))
    print('read_graph: {}s'.format(read_graph))
    print('connect: {}s'.format(connect))
    print('begin: {}s'.format(begin))
    print('vote: {}s'.format(vote))
    print('in: {}s'.format(inin))
    print('exec_each: {}s'.format(exec_each))
    print('out: {}s'.format(outout))
    print('exec_diagonal: {}s'.format(exec_diagonal))
    print('disconnect: {}s'.format(disconnect))
    print('overall: {}s'.format(overall))
    print('save_result: {}s'.format(save_result))

if __name__ == '__main__':
    log_file_name = sys.argv[1]
    analyze(log_file_name)