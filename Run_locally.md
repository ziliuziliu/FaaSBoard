# Run a Graph Processing Task Locally

## Launch the proxy_server
After preprocessing completes, launch the proxy server:

```bash
./proxy_server <PROXY_IP> <CORES>
    -- PROXY_IP: proxy server IP address
    -- CORES: number of CPU cores to use
# example
./proxy_server --proxy_ip 127.0.0.1 -cores 16 --v 1 > proxy.txt 2>&1
```

## Run the workers concurrently

```bash
# Run BFS on the LiveJournal dataset with 4 partitions
sudo numactl --physcpubind=0,1 ./local_bfs -request_id 101 -bfs_root 0 -partition_id 0 -graph_dir ../data/livejournal/unweighted/0 -result_dir ../data/result -cores 2 --out-csr --proxy_ip 127.0.0.1 --save_mode 1 --v 1 > 0.txt 2>&1 & \
sudo numactl --physcpubind=2,3 ./local_bfs -request_id 101 -bfs_root 0 -partition_id 1 -graph_dir ../data/livejournal/unweighted/1 -result_dir ../data/result -cores 2 --out-csr --proxy_ip 127.0.0.1 --save_mode 1 --v 1 > 1.txt 2>&1 & \
sudo numactl --physcpubind=4,5 ./local_bfs -request_id 101 -bfs_root 0 -partition_id 2 -graph_dir ../data/livejournal/unweighted/2 -result_dir ../data/result -cores 2 --out-csr --proxy_ip 127.0.0.1 --save_mode 1 --v 1 > 2.txt 2>&1 & \
sudo numactl --physcpubind=6,7 ./local_bfs -request_id 101 -bfs_root 0 -partition_id 3 -graph_dir ../data/livejournal/unweighted/3 -result_dir ../data/result -cores 2 --out-csr --proxy_ip 127.0.0.1 --save_mode 1 --v 1 > 3.txt 2>&1 &
```

Common flags used in the commands above:

```text
--request_id: request identifier
--partition_id: partition identifier
--bfs_root: BFS source vertex
--graph_dir: graph dataset directory in CSR binary format
--result_dir: output directory for graph processing results
--out-csr: enable out-CSR mode
--save_mode: result save mode (0: do not save, 1: local disk, 2: S3)
```

For more information about flags, see `FaaSBoard/include/util/flags.h`.
