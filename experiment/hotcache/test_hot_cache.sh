#!/bin/bash

# Path to log file
LOGFILE="script.log"

# Total run times
TOTAL_RUNS=21

# Make sure the script can be terminated
echo "The script will run $TOTAL_RUNS times, every 30 seconds."
echo "Logs will be recorded in $LOGFILE."

# Loop to run the script
for ((i=1; i<=TOTAL_RUNS; i++)); do
    # Get current timestamp
    CURRENT_TIME=$(date +"%Y-%m-%d %H:%M:%S")

    # Generate a random request_id between 0 and 20
    REQUEST_ID=$((i % 22))

    # Log the execution
    echo "[$CURRENT_TIME] Run #$i: ../script/run.sh -mode aws -request_id=$REQUEST_ID" >> "$LOGFILE"

    # Run the command and append output to the log file
    (cd ../build && ./../script/run.sh -mode aws -request_id "$REQUEST_ID") >> "$LOGFILE" 2>&1

    # Exit loop after all runs
    if [ "$i" -eq "$TOTAL_RUNS" ]; then
        echo "All $TOTAL_RUNS runs completed."
        break
    fi

    # Wait for 60 min
    sleep 6000
done
