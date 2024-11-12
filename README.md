# FaaSBoard

## Dependency 

```
    OpenMP
    aws-lambda-runtime
    glog
    gflags
```

## Structure

-   `build` : Compiled testing programs and applications.
-   `data` : Store the results of graph computing. 
-   `include` : Configurations and implementations of the graph computing system.
    -   `app` : Applications to run on the system.
    -   `communication` : Set-up for communications between different worker.
    -   `compute` : Define graph and graph set with their computing functions.
    -   `preprocess` : Functions used in preprocess to storing, partitioning and simulating.
    -   `util` : Provide basic classes, micros and function utilities.
        -   `mpmc` : Handle the concurrency queue of multiple producers and customers.
        -   `spsc` : Handle the concurrency queue of single producer and customer.
-   `original` : Contain the original datasets.
-   `script` : Scripts used to test cpus, upload codes, analyze logs and so on.
-   `test` : Run the services of the system.
    -   `others` : Basic test files of correctness, cpu, weight and so on.
    -   `preprocess` : Build programs to preprocess graph dataset and save into menory.

## Installation

In every host, clone the repository, enter this project and run:

```bash
    mkdir build
    cd build
    cmake ..
    make
```
This will compile and build the test and applications.

## Download Graph Dataset and Build CSR

### Download and Build CSR for livejournal or twitter

### For Livejournal Dataset

Download from url, then extract the raw txt: 

```bash
gzip -d <DATA_TXT_GZ_PATH>
```

Move the dataset file into the `original` directory under the repo:

```bash
mv <DATA_TXT> FaaSBoard/original
```

Enter the build directory and run the preprocess program firstly to handle the raw data before running:

```bash
# Enter running directory
cd build
# Start preprocessing
./preprocess_and_save -graph_file ../original/soc-LiveJournal1.txt -graph_root_dir ../data/livejournal -vertices 4847571 -edges 68993773 -partitions 4 --v 1
```

Once preprocessing ends, run the server program to set up the meta server and proxy server:

```bash
# Meta server is the worker storing the data
./meta_server -proxy_server_list 127.0.0.1 --v 1
# Proxy server config is in proxy.txt
./proxy_server -cores 16 --v 1 > proxy.txt 2>&1
```

 Then run the system concurrently:

```bash
# Run bfs on livejournal dataset
./test_bfs -request_id 123 -bfs_root 0 -graph_dir ../data/livejournal/0 -cores 4 --v 1 > 0.txt 2>&1 & ./test_bfs -request_id 123 -bfs_root 0 -graph_dir ../data/livejournal/1 -cores 4 --v 1 > 1.txt 2>&1 & ./test_bfs -request_id 123 -bfs_root 0 -graph_dir ../data/livejournal/2 -cores 4 --v 1 > 2.txt 2>&1 & ./test_bfs -request_id 123 -bfs_root 0 -graph_dir ../data/livejournal/3 -cores 4 --v 1 > 3.txt 2>&1
```

Prompts for flags in above commands:

```
	-- graph_root_dir: root directory for graph dataset in csr binary.
	-- graph_file: original graph dataset file.
	-- vertices: #vertices.
	-- edge: #edges.
	-- request_id: request id.
	-- bfs_root: root vertex for bfs.
	-- graph_dir: directory for graph dataset in csr binary.
	-- proxy_server_list: list of available proxy server addresses (ip) separated by comma.
	-- cores: cores to use.
	-- application: application type (bfs, cc, pr, sssp).
	-- no_pipeline: no in-exec_each-out-exec_diagonal pipeline.
```



### URLs

1) livejournal: https://snap.stanford.edu/data/soc-LiveJournal1.html
2) twitter: https://snap.stanford.edu/data/twitter-2010.html

## Basic Functionality Test

In this example, we run the basic single thread algorithm to check the correctness of concurrent one in single-host environment.

1) Open a terminal, enter the working directory:

```bash
cd build
```

2) Build the programs:

```bash
cmake ..
make
```

3) Run the test files:

```bash
./correctness_test -graph_file ../original/soc-LiveJournal1.txt -graph_root_dir ../data/livejournal -vertices 4847571 -edges 68993773 -application bfs -bfs_root 0 --v 1
```

The script will invoke a graph processing query. After a while, the script will return the results.
