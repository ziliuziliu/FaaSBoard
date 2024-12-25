#!/bin/bash
# Default values
BFS_ROOT=0
META_SERVER_IP="127.0.0.1"
REQUEST_ID=123
CORES=2
V_FLAG=1
SPARSE_ONLY="--sparse-only"
DENSE_ONLY="--dense-only"
LOG_DIR="./logs"
PR_ITERATIONS=20
GRAPH="livejournal"
APP="bfs"
MODE="local"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -bfs_root)
      BFS_ROOT="$2"
      shift 2
      ;;
    -meta_server)
      META_SERVER_IP="$2"
      shift 2
      ;;
    -request_id)
      REQUEST_ID="$2"
      shift 2
      ;;
    -cores)
      CORES="$2"
      shift 2
      ;;
    -v)
      V_FLAG="$2"
      shift 2
      ;;
    -pr_iterations)
      PR_ITERATIONS="$2"
      shift 2
      ;;
    -mode)
      MODE="$2"  # mode can be "local" or "aws"
      shift 2
      ;;
    -app)
      APP="$2"  # app can be "bfs" or "pr"
      shift 2
      ;;
    -graph)
      GRAPH="$2" # graph can be "livejournal" or "twitter-2010-balance"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Build the log directory
mkdir -p "$LOG_DIR"

# Bind CPU cores and set graph directories and output files
CPU_BINDINGS=(0,1 2,3 4,5 6,7 8,9 10,11)
GRAPH_DIRS=("../data/$GRAPH/0" "../data/$GRAPH/1" "../data/$GRAPH/2" "../data/$GRAPH/3" "../data/$GRAPH/4" "../data/$GRAPH/5")
OUTPUT_FILES=("$LOG_DIR/0.txt" "$LOG_DIR/1.txt" "$LOG_DIR/2.txt" "$LOG_DIR/3.txt" "$LOG_DIR/4.txt" "$LOG_DIR/5.txt")

# AWS function names and result files
AWS_FUNCTIONS=("${APP}_0" "${APP}_1" "${APP}_2" "${APP}_3" "${APP}_4" "${APP}_5")
AWS_RESULTS=("result0.txt" "result1.txt" "result2.txt" "result3.txt" "result4.txt" "result5.txt")

if [[ "$MODE" == "local" ]]; then
  # Set the execution command and additional flags for 'local' mode
  if [[ "$APP" == "bfs" ]]; then
    EXEC_CMD="./local_bfs -request_id $REQUEST_ID -bfs_root $BFS_ROOT -cores $CORES --meta-server $META_SERVER_IP --v $V_FLAG"
    ADDITIONAL_FLAG=$SPARSE_ONLY
  elif [[ "$APP" == "pr" ]]; then
    EXEC_CMD="./local_pr -request_id $REQUEST_ID -pr_iterations $PR_ITERATIONS -cores $CORES --meta-server $META_SERVER_IP --v $V_FLAG"
    ADDITIONAL_FLAG=$DENSE_ONLY
  else
    echo "Invalid app. Please choose either 'bfs' or 'pr'."
    exit 1
  fi

  # Run the commands in the background
  for i in "${!CPU_BINDINGS[@]}"; do
    CMD="sudo numactl --physcpubind=${CPU_BINDINGS[$i]} $EXEC_CMD -graph_dir ${GRAPH_DIRS[$i]} $ADDITIONAL_FLAG > ${OUTPUT_FILES[$i]} 2>&1 &"
    echo "Running: $CMD"
    eval "$CMD"
  done
  echo "All local tasks are running in the background."

elif [[ "$MODE" == "aws" ]]; then
  # Set AWS Lambda payload flags
  if [[ "$APP" == "bfs" ]]; then
    SPARSE_ONLY=true
    DENSE_ONLY=false
  elif [[ "$APP" == "pr" ]]; then
    SPARSE_ONLY=false
    DENSE_ONLY=true
  else
    echo "Invalid app. Please choose either 'bfs' or 'pr'."
    exit 1
  fi

  # Execute AWS Lambda functions
  for i in "${!AWS_FUNCTIONS[@]}"; do
    PAYLOAD=$(cat <<EOF
{
  "graph_dir": "graph",
  "result_dir": "/tmp",
  "meta_server": "172.31.12.143",
  "cores": 2,
  "no_pipeline": false,
  "sparse_only": true,
  "dense_only": false,
  "save_mode": 2,
  "s3_bucket": "ziliuziliu",
  "request_id": 123,
  "bfs_root": 0
}
EOF
)
    CMD="aws lambda invoke --function-name ${AWS_FUNCTIONS[$i]} --payload '$PAYLOAD' ${AWS_RESULTS[$i]} &"
    echo "Running: $CMD"
    eval "$CMD"
  done
  echo "All AWS Lambda tasks are running in the background."
else
  echo "Invalid mode. Please choose either 'local' or 'aws'."
  exit 1
fi
