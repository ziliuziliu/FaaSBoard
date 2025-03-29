#!/bin/bash
set -euo pipefail

TOTAL_RUNS=11
INTERVAL=600  # Interval seconds, example value

# 创建基于间隔时间的日志目录
LOG_DIR="$HOME/FaaSBoard/build/cachelog/$INTERVAL"
mkdir -p "$LOG_DIR"

echo "Script will run $TOTAL_RUNS times, every $INTERVAL seconds."
echo "Logging to $LOG_DIR."

for ((i=1; i<=TOTAL_RUNS; i++)); do
    CURRENT_TIME=$(date +"%Y-%m-%d %H:%M:%S")
    REQUEST_ID=420  # Default ID

    # 每次运行的日志文件名包含运行编号
    LOGFILE="$LOG_DIR/script_run_$i.log"

    echo "[$CURRENT_TIME] Run #$i: run.sh $REQUEST_ID" >> "$LOGFILE"

    (cd ~/FaaSBoard/script && bash run.sh "$REQUEST_ID") >> "$LOGFILE" 2>&1

    if [ "$i" -lt "$TOTAL_RUNS" ]; then
        sleep "$INTERVAL"
    fi
done

echo "All $TOTAL_RUNS runs completed." >> "$LOGFILE"