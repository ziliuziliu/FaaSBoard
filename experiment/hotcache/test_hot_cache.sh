#!/bin/bash
set -euo pipefail

LOGFILE="script.log"
TOTAL_RUNS=11
INTERVAL=30  # Interval seconds

echo "Script will run $TOTAL_RUNS times, every $INTERVAL seconds."
echo "Logging to $LOGFILE."

for ((i=1; i<=TOTAL_RUNS; i++)); do
    CURRENT_TIME=$(date +"%Y-%m-%d %H:%M:%S")
    REQUEST_ID=$((RANDOM % 11))  # Random number between 0-10

    echo "[$CURRENT_TIME] Run #$i: run.sh $REQUEST_ID" >> "$LOGFILE"

    (cd ~/FaaSBoard/script && bash run.sh "$REQUEST_ID") >> "$LOGFILE" 2>&1

    if [ "$i" -lt "$TOTAL_RUNS" ]; then
        sleep "$INTERVAL"
    fi
done

echo "All $TOTAL_RUNS runs completed." >> "$LOGFILE"