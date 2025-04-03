#!/bin/bash
set -euo pipefail

TOTAL_RUNS=11
INTERVAL=1200  # Interval seconds, example value

# 创建基于间隔时间的日志目录
LOG_DIR="$HOME/FaaSBoard/build/cachelog/$INTERVAL"
mkdir -p "$LOG_DIR"

echo "Script will run $TOTAL_RUNS times, every $INTERVAL seconds (two instances per run)."
echo "Logging to $LOG_DIR."

for ((i=1; i<=TOTAL_RUNS; i++)); do
    CURRENT_TIME=$(date +"%Y-%m-%d %H:%M:%S")
    REQUEST_ID1=420  # First instance ID
    REQUEST_ID2=421  # Second instance ID
    REQUEST_ID3=422  # Third instance ID

    # 为两个并行实例创建单独的日志文件
    LOGFILE1="$LOG_DIR/script_run_${i}_1.log"
    LOGFILE2="$LOG_DIR/script_run_${i}_2.log"
    LOGFILE3="$LOG_DIR/script_run_${i}_3.log"

    echo "[$CURRENT_TIME] Run #$i: Instance 1 - run.sh $REQUEST_ID1" >> "$LOGFILE1"
    echo "[$CURRENT_TIME] Run #$i: Instance 2 - run.sh $REQUEST_ID2" >> "$LOGFILE2"
    echo "[$CURRENT_TIME] Run #$i: Instance 3 - run.sh $REQUEST_ID3" >> "$LOGFILE3"

    # 清除缓存
    aws lambda invoke --function-name update-fargate-endpoints --payload '{"cluster_name": "faasboard", "service_name": "proxy_server", "list_key": "GlobalIPList"}' output.txt
    valkey-cli --tls -h faasboard-hcdnu5.serverless.apse1.cache.amazonaws.com -p 6379 DEL 420
    valkey-cli --tls -h faasboard-hcdnu5.serverless.apse1.cache.amazonaws.com -p 6379 DEL 421
    valkey-cli --tls -h faasboard-hcdnu5.serverless.apse1.cache.amazonaws.com -p 6379 DEL 422

    # 启动两个并行的实例，使用 & 在后台运行第一个实例
    (cd ~/FaaSBoard/script && bash run.sh "$REQUEST_ID1") >> "$LOGFILE1" 2>&1 &
    (cd ~/FaaSBoard/script && bash run.sh "$REQUEST_ID2") >> "$LOGFILE2" 2>&1 &
    (cd ~/FaaSBoard/script && bash run.sh "$REQUEST_ID3") >> "$LOGFILE3" 2>&1

    # 等待所有后台任务完成
    wait

    if [ "$i" -lt "$TOTAL_RUNS" ]; then
        sleep "$INTERVAL"
    fi
done

echo "All $TOTAL_RUNS runs completed with two instances each run."