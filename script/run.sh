#!/bin/bash
# Example: ./run.sh -mode local -app bfs -graph livejournal -bfs_root 0 -proxy_ip 127.0.0.1
# Example: sudo ./run.sh -mode local -app pr -graph twitter -pr_iterations 20 -proxy_ip 127.0.0.1
# Example: ./run.sh -mode aws -app pr -graph twitter -pr_iterations 20 
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
LOG_DIR="$ROOT_DIR/script/logs"

MODE=local
APP=bfs
GRAPH=livejournal
PARTITIONS=0
REQUEST_ID=101
CORES=2
V=1
BFS_ROOT=0
SSSP_ROOT=0
PR_ITERATIONS=20
PROXY_IP=127.0.0.1
RESULT_DIR="$ROOT_DIR/data/result"
FUNCTION_PREFIX=""
S3_BUCKET="ziliuziliu"
SAVE_MODE=0
DYNAMIC_INVOKE=false
ELASTIC_PROXY=true
ELASTICACHE_HOST="faasboard-hcdnu5.serverless.apse1.cache.amazonaws.com" # you need to update this with your own Elasticache host if using elastic proxy
KILL_WAIT_MS=300

usage() {
  echo "Usage: ./run.sh -mode <local|aws> -app <bfs|pr|sssp|cc> [-graph livejournal|twitter|friendster|rmat27] [options]"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -mode) MODE="$2"; shift 2 ;;
    -app) APP="$2"; shift 2 ;;
    -graph) GRAPH="$2"; shift 2 ;;
    -partitions) PARTITIONS="$2"; shift 2 ;;
    -request_id) REQUEST_ID="$2"; shift 2 ;;
    -cores) CORES="$2"; shift 2 ;;
    -bfs_root) BFS_ROOT="$2"; shift 2 ;;
    -sssp_root) SSSP_ROOT="$2"; shift 2 ;;
    -pr_iterations) PR_ITERATIONS="$2"; shift 2 ;;
    -proxy_ip) PROXY_IP="$2"; shift 2 ;;
    -result_dir) RESULT_DIR="$2"; shift 2 ;;
    -function_prefix) FUNCTION_PREFIX="$2"; shift 2 ;;
    -s3_bucket) S3_BUCKET="$2"; shift 2 ;;
    -save_mode|-save-mode) SAVE_MODE="$2"; shift 2 ;;
    -dynamic_invoke) DYNAMIC_INVOKE="$2"; shift 2 ;;
    -elastic_proxy) ELASTIC_PROXY="$2"; shift 2 ;;
    -elasticache_host) ELASTICACHE_HOST="$2"; shift 2 ;;
    -kill_wait_ms) KILL_WAIT_MS="$2"; shift 2 ;;
    -v) V="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "Unknown option: $1"; usage ;;
  esac
done

case "$GRAPH:$APP" in
  livejournal:*) DEFAULT_PARTITIONS=1 ;;
  twitter:bfs|twitter:pr) DEFAULT_PARTITIONS=6 ;;
  twitter:cc|twitter:sssp) DEFAULT_PARTITIONS=12 ;;
  friendster:bfs|friendster:pr) DEFAULT_PARTITIONS=8 ;;
  friendster:cc|friendster:sssp) DEFAULT_PARTITIONS=14 ;;
  rmat27:bfs|rmat27:pr) DEFAULT_PARTITIONS=9 ;;
  rmat27:cc|rmat27:sssp) DEFAULT_PARTITIONS=17 ;;
  *) echo "Unsupported graph/app: $GRAPH / $APP"; exit 1 ;;
 esac
[[ "$PARTITIONS" -gt 0 ]] || PARTITIONS="$DEFAULT_PARTITIONS"

case "$APP" in
  bfs)
    SUBDIR=unweighted; LOCAL_FLAGS=(--out-csr -bfs_root "$BFS_ROOT"); AWS_OUT=true; AWS_IN=false; NEED_DEG=false; EXTRA_JSON=", \"bfs_root\": $BFS_ROOT" ;;
  pr)
    SUBDIR=unweighted; LOCAL_FLAGS=(--in-csr --need-global-degree -pr_iterations "$PR_ITERATIONS"); AWS_OUT=false; AWS_IN=true; NEED_DEG=true; EXTRA_JSON=", \"pr_iterations\": $PR_ITERATIONS" ;;
  sssp)
    SUBDIR=weighted; LOCAL_FLAGS=(--out-csr -sssp_root "$SSSP_ROOT"); AWS_OUT=true; AWS_IN=false; NEED_DEG=false; EXTRA_JSON=", \"sssp_root\": $SSSP_ROOT" ;;
  cc)
    SUBDIR=undirected; LOCAL_FLAGS=(--out-csr); AWS_OUT=true; AWS_IN=false; NEED_DEG=false; EXTRA_JSON="" ;;
  *) echo "Unsupported app: $APP"; exit 1 ;;
esac

mkdir -p "$LOG_DIR" "$RESULT_DIR"
[[ -n "$FUNCTION_PREFIX" ]] || FUNCTION_PREFIX="${GRAPH}_${APP}"

cpu_bind() {
  local start=$(( $1 * CORES ))
  local end=$(( start + CORES - 1 ))
  seq -s, "$start" "$end"
}

if [[ "$MODE" == "local" ]]; then
  for ((i=0; i<PARTITIONS; i++)); do
    cmd=(sudo numactl --physcpubind="$(cpu_bind "$i")" "$BUILD_DIR/local_$APP"
      -request_id "$REQUEST_ID" -partition_id "$i"
      -graph_dir "$ROOT_DIR/data/$GRAPH/$SUBDIR/$i" -result_dir "$RESULT_DIR"
      -cores "$CORES" --proxy_ip "$PROXY_IP" --save-mode "$SAVE_MODE" --v "$V"
      "${LOCAL_FLAGS[@]}")
    echo "Running: nohup ${cmd[*]} > $LOG_DIR/${APP}_${i}.txt 2>&1 &"
    nohup "${cmd[@]}" > "$LOG_DIR/${APP}_${i}.txt" 2>&1 &
  done
  echo "Local workers launched. Start $BUILD_DIR/proxy_server separately if needed."
elif [[ "$MODE" == "aws" ]]; then
  for ((i=0; i<PARTITIONS; i++)); do
    fn="${FUNCTION_PREFIX}_${i}"
    proxy_json=", \"elastic_proxy\": $ELASTIC_PROXY"
    if [[ "$ELASTIC_PROXY" == "true" ]]; then
      proxy_json+=" , \"elasticache_host\": \"$ELASTICACHE_HOST\""
    else
      proxy_json+=" , \"proxy_ip\": \"$PROXY_IP\""
    fi
    payload="{\"function_name\":\"$fn\",\"graph_dir\":\"graph\",\"result_dir\":\"/tmp\",\"cores\":$CORES,\"no_pipeline\":false,\"out_csr\":$AWS_OUT,\"in_csr\":$AWS_IN,\"need_global_degree\":$NEED_DEG,\"save_mode\":0,\"s3_bucket\":\"$S3_BUCKET\",\"request_id\":$REQUEST_ID,\"dynamic_invoke\":$DYNAMIC_INVOKE,\"partition_id\":$i,\"kill_wait_ms\":$KILL_WAIT_MS$proxy_json$EXTRA_JSON}"
    echo "Running: nohup aws lambda invoke --function-name $fn ... $LOG_DIR/${fn}.json &"
    nohup aws lambda invoke --function-name "$fn" --cli-binary-format raw-in-base64-out --payload "$payload" "$LOG_DIR/${fn}.json" > "$LOG_DIR/${fn}_error.log" 2>&1 &
  done
  echo "AWS Lambda invocations launched in the background."
else
  echo "Invalid mode: $MODE"
  usage
fi
