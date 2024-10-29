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
-   `inlucde` : Header files of the configurations of the graph computing system.
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
./preprocess_and_save -graph_file ../original/soc-LiveJournal1.txt -graph_root_dir ../data/livejournal -vertices 4847571 -edges 68993773 --v 1
```

Once preprocessing ends, you can run the system concurrently in one host:

```bash
# Run bfs on livejournal dataset
./test_bfs -request_id 123 -bfs_root 0 -graph_dir ../data/livejournal/0 -cores 4 --v 1 > 0.txt 2>&1 & ./test_bfs -request_id 123 -bfs_root 0 -graph_dir ../data/livejournal/1 -cores 4 --v 1 > 1.txt 2>&1 & ./test_bfs -request_id 123 -bfs_root 0 -graph_dir ../data/livejournal/2 -cores 4 --v 1 > 2.txt 2>&1 & ./test_bfs -request_id 123 -bfs_root 0 -graph_dir ../data/livejournal/3 -cores 4 --v 1 > 3.txt 2>&1
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

## Distributed Deployment

To check the aws cpus and set up the servers:

1. Build the testing files in `build` directory.

```bash
make aws-lambda-package-cpu_test
```

2. Run the meta server program with IP of the worker storing the data.

```bash
./meta_server -proxy_server_list 127.0.0.1 --v 1
```

3. Run the proxy with configuration in `proxy.txt`

````bash
./proxy_server -cores 16 --v 1 > proxy.txt 2>&1
````

