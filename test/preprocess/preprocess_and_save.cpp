#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "preprocess/partition_strategy.h"
#include "util/types.h"
#include "util/log.h"
#include "util/flags.h"

#include <cstring>
#include <cmath>

template <typename T>
int main_impl(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;

    raw_graph<T> g(FLAGS_vertices, FLAGS_edges * (FLAGS_undirected ? 2 : 1));
    // g.read_txt(FLAGS_graph_file, FLAGS_undirected);
    // g.print();
    VLOG(1) << "reading directly from CSR";
    g.read_csr(FLAGS_graph_file + ".csr.in", FLAGS_graph_file + ".csr.out");

    std::string strategy = FLAGS_strategy;
    VLOG(1) << "partition_strategy: " << strategy;
    int total_block = FLAGS_partitions;
    partition_strategy<T> partition_handler(g, total_block);

    try {
        partition_handler.execute_partition(strategy);
    } catch (const std::invalid_argument& e) {
        VLOG(1) << "Invalid partitioning strategy: " << e.what() << std::endl;
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what() << std::endl;
        return -1;
    }

    return 0;
}

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::string edge_weight_type = FLAGS_ewT;

    if (edge_weight_type == "empty") {
        return main_impl<empty>(argc, argv);
    } else if (edge_weight_type == "int") {
        return main_impl<int>(argc, argv);
    } else if (edge_weight_type == "uint32_t") {
        return main_impl<uint32_t>(argc, argv);
    } else if (edge_weight_type == "float") {
        return main_impl<float>(argc, argv);
    } else {
        VLOG(1) << "Unsupported edge weight type: " << edge_weight_type << std::endl;
        return -1;
    }
}