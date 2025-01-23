#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/types.h"
#include "util/log.h"
#include "util/flags.h"

#include <cstring>

template <typename T>
int main_impl(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;

    raw_graph<T> g(FLAGS_vertices, FLAGS_edges * (FLAGS_undirected ? 2 : 1));
    g.read_txt(FLAGS_graph_file, FLAGS_undirected);
    // g.print();

    partition_result result;
    std::vector<graph_set<T> *> graphsets;
    int total_block = FLAGS_partitions, cut;

    VLOG(1) << "optimal cut + binpack";
    cut = sqrt((double)total_block) + 1;
    result = g.checkerboard_partition(cut);
    result.print();
    double unbalance_ratio = result.get_unbalance_ratio();
    VLOG(1) << "unbalance ratio: " << unbalance_ratio;
    VLOG(1) << "begin partitioning";
    graphsets = g.partition(result);
    for (auto graphset : graphsets) {
        graphset->print(false);
    }
    for (;;) {
        try {
            double balance_ratio;
            std::string save;
            VLOG(1) << "balance ratio?";
            std::cin >> balance_ratio;
            std::vector<graph_set<T> *> new_graphsets = graph_set<T>::binpack(graphsets, total_block, balance_ratio);
            for (auto graphset : new_graphsets) {
                graphset->print(false);
            }
            graph_set<T>::simulation(new_graphsets);
            VLOG(1) << "save? (Y or N)";
            std::cin >> save;
            if (save == "Y") {
                double total_resource;
                VLOG(1) << "total resource (cores)?";
                std::cin >> total_resource;
                graph_set<T>::save(new_graphsets, total_resource);
                break;
            }
        } catch (const std::runtime_error &e) {
            std::cout << e.what() << std::endl;
        }
    }

    return 0;
}

int main(int argc, char *argv[]) {
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