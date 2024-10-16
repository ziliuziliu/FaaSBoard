#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/types.h"
#include "util/log.h"

#include <cstring>

int main(int argc, char *argv[]) {

    google::InitGoogleLogging(argv[0]);

    raw_graph<empty> g(41652230, 1468365182);
    g.read_csr("/data/twitter-2010.csr");

    partition_result result;
    std::vector<graph_set<empty> *> graphsets;
    int total_block = 16, cut;

    VLOG(1) << "naive cut + binpack";
    cut = sqrt((double)total_block) + 1;
    result = g.naive_checkerboard_partition(cut);
    VLOG(1) << "begin partitioning";
    graphsets = g.partition(result);
    VLOG(1) << "end partitioning";
    for (;;) {
        try {
            double balance_ratio;
            std::cout << "balance ratio?";
            std::cin >> balance_ratio;
            if (balance_ratio == 0.0)
                break;
            std::vector<graph_set<empty> *> current_graphsets(graphsets), new_graphsets;
            new_graphsets = graph_set<empty>::binpack(current_graphsets, total_block, balance_ratio);
            graph_set<empty>::simulation(new_graphsets);
        } catch (const std::runtime_error &e) {
            LOG(FATAL) << e.what() << std::endl;
        }
    }

    return 0;
}