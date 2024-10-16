#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/types.h"
#include "util/log.h"
#include "util/flags.h"

#include <cstring>

int main(int argc, char *argv[]) {

    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;

    raw_graph<empty> g(FLAGS_vertices, FLAGS_edges);
    g.read_txt(FLAGS_graph_file);
    // g.print();

    partition_result result;
    std::vector<graph_set<empty> *> graphsets;
    int total_block = 4, cut;

    VLOG(1) << "optimal cut + binpack";
    cut = sqrt((double)total_block) + 1;
    result = g.checkerboard_partition(cut);
    result.print();
    double unbalance_ratio = result.get_unbalance_ratio();
    VLOG(1) << "unbalance ratio: " << unbalance_ratio;
    VLOG(1) << "begin partitioning";
    graphsets = g.partition(result);
    for (auto graphset : graphsets) {
        graphset -> print(false);
    }
    for (;;) {
        try {
            double balance_ratio;
            std::string save;
            std::cout << "balance ratio?" << std::endl;
            std::cin >> balance_ratio;
            std::vector<graph_set<empty> *> new_graphsets = graph_set<empty>::binpack(graphsets, total_block, balance_ratio);
            for (auto graphset : new_graphsets) {
                graphset -> print(false);
            }
            graph_set<empty>::simulation(new_graphsets);
            std::cout << "save? (Y or N)" << std::endl;
            std::cin >> save;
            if (save == "Y") {
                graph_set<empty>::save(new_graphsets, FLAGS_graph_root_dir);
                break;
            }
        } catch (const std::runtime_error &e) {
            std::cout << e.what() << std::endl;
        }
    }

    return 0;
}