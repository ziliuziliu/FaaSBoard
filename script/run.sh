#!/bin/bash

# config
COMMON_PAYLOAD='{
  "graph_dir": "graph",
  "result_dir": "/tmp",
  "cores": 2,
  "no_pipeline": false,
  "sparse_only": true,
  "dense_only": false,
  "need_global_degree": false,
  "save_mode": 0,
  "s3_bucket": "ziliuziliu",
  "request_id": 100,
  "bfs_root": 0,
  "dynamic_invoke": false,
  "elastic_proxy": true,
  "elasticache_host": "faasboard-hcdnu5.serverless.apse1.cache.amazonaws.com"
}'

# launch 8 parallel Lambda invocations
for i in {0..7}; do
  aws lambda invoke \
    --function-name cache_bfs_$i \
    --payload "$(echo $COMMON_PAYLOAD | jq --argjson pid $i --arg fn cache_bfs_$i '. + { "function_name": $fn, "partition_id": $pid }')" \
    result${i}.txt &
done

# wait for all background jobs
wait
echo "All invocations completed."