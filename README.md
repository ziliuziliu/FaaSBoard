# FaaSBoard

[![Static Badge](https://img.shields.io/badge/Organization_Website-EPCC-purple)](http://epcc.sjtu.edu.cn)

## Introduction

FaaSBoard is a scalable, efficient, and cost-effective graph processing framework powered by serverless computing and deployed on AWS serverless infrastructure.

FaaSBoard packages compiled graph processing code and partitioned graph data into images to construct AWS Lambda functions, where multiple Lambda functions collaborate to complete a graph processing task. The image-based graph loading approach significantly accelerates data loading during warm starts. Since serverless functions cannot communicate directly, we introduce a proxy server to handle communication logic, implementing computation-communication interleaving and message consolidation. Meanwhile, we propose a triple-mode vector format to balance different communication vector formats and optimize overall performance. Furthermore, the 2D balanced partitioning strategy achieves better load balancing while maintaining $O(\sqrt{p})$ communication complexity, making it more suitable for resource-constrained serverless environments. Additionally, we observe the inherent intra-job elasticity of graph processing tasks and leverage proactive termination and respawn to further reduce monetary cost.

## Hardware and Software Configuration

FaaSBoard runs on AWS serverless infrastructure and also supports local single-host execution for testing. We especially recommend testing on AWS EC2.

1. **Local execution**: We recommend a host machine with at least 40 CPU cores, 120 GB memory, and 150 GB SSD storage. The reference operating system is Ubuntu 22.04. 
2. **AWS deployment**: The proxy server runs on Ubuntu 22.04, and Lambda functions are built from the `public.ecr.aws/lambda/provided:al2023` base image. Each Lambda function is configured with 2 vCPUs and 3538 MB memory.

## Structure

- `build`: Compiled test programs and applications.
- `data`: Graph datasets and graph processing results.
- `include`: Core configurations and implementations of the graph processing system.
  - `app`: Applications that run on the system.
  - `communication`: Communication setup for workers.
  - `compute`: Graph data structures and compute kernels.
  - `preprocess`: Functions for preprocessing, storage, partitioning, and simulation.
  - `util`: Common classes, macros, and utility functions.
    - `mpmc`: Multi-producer, multi-consumer concurrent queues.
    - `spsc`: Single-producer, single-consumer concurrent queues.
- `script`: Scripts for environment setup, dataset preparation, utilities, CPU tests, code upload.
    - `analyze`: Scripts for log analysis
- `test`: System test programs.
  - `others`: Basic tests for correctness, CPU behavior, weights, and related functionality.
  - `preprocess`: Programs for preprocessing graph datasets and storing them in memory.

## Dependency and Installation

To configure the runtime environment and install dependencies, clone the repository, enter the project directory, and run:

```bash
cd script
./prepare_machine.sh
```

This installs `Docker`, `AWS SDK`, `glog`, and other required dependencies.

To compile the test programs and applications:

```bash
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=~/lambda-install
make
```

To build AWS Lambda code packages:

```bash
make aws-lambda-package-lambda_bfs
make aws-lambda-package-lambda_pr
make aws-lambda-package-lambda_sssp
make aws-lambda-package-lambda_cc
```

## Workflow

### Download a Graph Dataset and Build CSR

Download the dataset and extract the raw text file:

```bash
gzip -d <DATA_TXT_GZ_PATH>
```

Move the dataset into the repository `data` directory:

```bash
mv <DATA_TXT> FaaSBoard/data/
```

You can also enter `FaaSBoard/script`, run this command to download download datasets, unzip that and move the datasets to `FaaSBoard/data`:
```bash
./prepare_dataset.sh
```

After datasets are ready, enter the build directory and preprocess the raw graph before running an application. Different applications require different preprocessing parameters; see `FaaSBoard/script/preprocess_graph.sh` for details. Run:

```bash
# Enter the build directory
cd build
# Start preprocessing
./preprocess_and_save <GRAPH_FILE> <GRAPH_ROOT_DIR> <VERTICES> <EDGES> <PARTITIONS>
   -- GRAPH_FILE: original graph dataset file
   -- GRAPH_ROOT_DIR: root directory of the graph dataset in CSR binary format
   -- VERTICES: num of vertices
   -- EDGES: number of edges
   -- PARTITIONS: number of partitions
# example
sudo ./preprocess_and_save -graph_file ../data/soc-LiveJournal1.txt -graph_root_dir ../data/livejournal/unweighted -vertices 4847571 -edges 68993773 -partitions 4 --v 1
```

### Run a Graph Processing Task Locally

See [Run Locally](Run_locally.md) for the complete local execution workflow.

### Basic Functionality Test Locally

The following example runs a basic single-threaded algorithm to validate the correctness of the concurrent implementation in a single-host environment.

1. Enter the build directory:

```bash
cd build
```

2. Run the correctness test:

```bash
./correctness_test <GRAPH_FILE> <GRAPH_ROOT_DIR> <VERTICES> <EDGES> <APPLICATION> <BFS_ROOT>
    -- GRAPH_FILE: original graph dataset file
    -- GRAPH_ROOT_DIR: root directory of the graph dataset in CSR binary format
    -- VERTICES: number of vertices
    -- EDGES: number of edges
    -- APPLICATION: graph application (for example, bfs)
# example
./correctness_test -graph_file ../original/soc-LiveJournal1.txt -graph_root_dir ../data/livejournal -vertices 4847571 -edges 68993773 -application bfs -bfs_root 0 --v 1
```

The program issues a graph processing query and prints the result after execution completes.

### Run a Graph Processing Task on AWS

See [Run on AWS](Run_on_aws.md) for the complete AWS deployment and execution workflow.

### Unified Launch Script

`FaaSBoard/script/run.sh` provides a compact entry point for both local execution and AWS Lambda invocation. It selects the proper dataset layout for each application automatically:

- `bfs`, `pr` -> `data/<graph>/unweighted`
- `sssp` -> `data/<graph>/weighted`
- `cc` -> `data/<graph>/undirected`

Basic usage:

```bash
cd script/
./run.sh -mode <local|aws> -app <bfs|pr|sssp|cc> -graph <livejournal|twitter|friendster|rmat27> [options]
```

However, you need to complete the preparations for executing the graph processing task locally/on AWS, as instructed previously, before you can call `FaaSBoard/script/run.sh`.

## Source of Graph Datasets

Special thanks to [SNAP](https://snap.stanford.edu/) for providing the datasets used in our experiments.

1. `livejournal`: https://snap.stanford.edu/data/soc-LiveJournal1.html
2. `twitter`: https://snap.stanford.edu/data/twitter-2010.html
3. `friendster`: https://snap.stanford.edu/data/com-Friendster.html
4. `rmat27`: `cd script/ && python gen_rmat27.py`


## Previous Work

```bibtex
@inproceedings{liu2024faasgraph,
  title={FaaSGraph: Enabling Scalable, Efficient, and Cost-Effective Graph Processing with Serverless Computing},
  author={Liu, Yushi and Sun, Shixuan and Li, Zijun and Chen, Quan and Gao, Sen and He, Bingsheng and Li, Chao and Guo, Minyi},
  booktitle={Proceedings of the 29th ACM International Conference on Architectural Support for Programming Languages and Operating Systems, Volume 2},
  pages={385--400},
  year={2024}
}
```
