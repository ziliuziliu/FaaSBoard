#!/bin/bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <request_id>"
  exit 1
fi

request_id=$1

graph_dir="graph"
result_dir="/tmp"
cores=2
no_pipeline=false
sparse_only=true
dense_only=false
need_global_degree=false
save_mode=0
s3_bucket="ziliuziliu"
bfs_root=0
dynamic_invoke=false
elastic_proxy=true
elasticache_host="faasboard-hcdnu5.serverless.apse1.cache.amazonaws.com"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

for i in {0..7}; do
  (
    output_file="result${i}.txt"

    payload="{
      \"function_name\": \"cache_bfs_$i\",
      \"graph_dir\": \"$graph_dir\",
      \"result_dir\": \"$result_dir\",
      \"cores\": $cores,
      \"no_pipeline\": $no_pipeline,
      \"sparse_only\": $sparse_only,
      \"dense_only\": $dense_only,
      \"need_global_degree\": $need_global_degree,
      \"save_mode\": $save_mode,
      \"s3_bucket\": \"$s3_bucket\",
      \"request_id\": $request_id,
      \"bfs_root\": $bfs_root,
      \"dynamic_invoke\": $dynamic_invoke,
      \"partition_id\": $i,
      \"elastic_proxy\": $elastic_proxy,
      \"elasticache_host\": \"$elasticache_host\"
    }"

    log "invoke cache_bfs_$i"
    aws lambda invoke \
      --function-name cache_bfs_$i \
      --payload "$payload" \
      $output_file > /dev/null

    if grep -q '"FunctionError"' "$output_file"; then
      log "cache_bfs_$i failed, see $output_file"
    else
      log "cache_bfs_$i completed"
    fi
  ) &
done

wait
log "all Lambda invocations completed"