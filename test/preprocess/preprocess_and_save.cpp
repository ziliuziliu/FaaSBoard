#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/types.h"
#include "util/log.h"
#include "util/flags.h"

#include <cstring>
#include <cmath>

int main(int argc, char *argv[]) {

    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;

    raw_graph<empty> g(FLAGS_vertices, FLAGS_edges * (FLAGS_undirected ? 2 : 1));
    g.read_txt(FLAGS_graph_file, FLAGS_undirected);
    // g.print();

    partition_result result;
    std::vector<graph_set<empty> *> graphsets;
    int total_block = FLAGS_partitions, cut;

    VLOG(1) << "optimal cut + binpack";
    if (int(std::sqrt(total_block)) * int(std::sqrt(total_block)) == total_block) {
        cut = std::sqrt(total_block);
    } else {
        cut = std::ceil(std::sqrt(total_block));
    }
    for (;;) {
        VLOG(1) << "cut: " << cut;
        result = g.checkerboard_partition(cut);
        result.print();
        VLOG(1) << "unbalance ratio: " << result.get_unbalance_ratio();;
        if ((int)result.count_non_empty() >= total_block) {
            break;
        }
        VLOG(1) << "not enough blocks, increase cut and try again";
        cut++;
    }
    VLOG(1) << "begin partitioning";
    graphsets = g.partition(result);
    for (auto graphset : graphsets) {
        graphset -> print(false);
    }
    for (;;) {
        try {
            double balance_ratio;
            std::string save;
            VLOG(1) << "balance ratio?";
            std::cin >> balance_ratio;
            std::vector<graph_set<empty> *> new_graphsets = graph_set<empty>::binpack(graphsets, total_block, balance_ratio);
            for (auto graphset : new_graphsets) {
                graphset -> print(false);
            }
            graph_set<empty>::simulation(new_graphsets);
            VLOG(1) << "save? (Y or N)";
            std::cin >> save;
            if (save == "Y") {
                double total_resource;
                VLOG(1) << "total resource (cores)?";
                std::cin >> total_resource;
                graph_set<empty>::save(new_graphsets, total_resource);
                break;
            }
        } catch (const std::runtime_error &e) {
            std::cout << e.what() << std::endl;
        }
    }

    return 0;
}