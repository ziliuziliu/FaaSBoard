#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/log.h"
#include "util/types.h"

#include <cstring>

int main() {

    raw_graph<empty> g(41652230, 1468365182);
    g.read_csr("/data/twitter-2010.csr");

    partition_result result;
    std::vector<graph_set<empty> *> graphsets;
    int total_block = 16, cut;

    LOG(INFO) << "naive cut + cyclic layout";
    cut = total_block;
    result = g.naive_checkerboard_partition(cut);
    LOG(INFO) << "begin partitioning";
    graphsets = g.partition(result);
    LOG(INFO) << "cycle placing";
    graphsets = graph_set<empty>::cycle(graphsets, total_block);
    graph_set<empty>::simulation(graphsets);

    return 0;
}