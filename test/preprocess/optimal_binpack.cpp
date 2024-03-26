#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/print.h"
#include "util/types.h"

#include <cstring>

int main() {

    raw_graph<empty> g(41652230, 1468365182);
    g.read_csr("/data/twitter-2010.csr");

    partition_result result;
    std::vector<graph_set<empty> *> graphsets;
    int total_block = 16, cut;

    print_log("optimal cut + binpack");
    cut = sqrt((double)total_block) + 1;
    result = g.checkerboard_partition(cut);
    print_log("begin partitioning");
    graphsets = g.partition(result);
    for (;;) {
        try {
            double balance_ratio;
            std::cout << "balance ratio?" << std::endl;
            std::cin >> balance_ratio;
            if (balance_ratio == 0.0)
                break;
            std::vector<graph_set<empty> *> new_graphsets = graph_set<empty>::binpack(graphsets, total_block, balance_ratio);
            graph_set<empty>::simulation(new_graphsets);
        } catch (const std::runtime_error &e) {
            std::cout << e.what() << std::endl;
        }
    }
    print_log("end partitioning");

    return 0;
}