#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/types.h"
#include "util/log.h"

#include <cstring>

int main() {

    raw_graph<empty> g(41652230, 1468365182);
    g.read_csr("/data/twitter-2010.csr");

    partition_result result;
    std::vector<graph_set<empty> *> graphsets;
    int total_block = 16;

    LOG(INFO) << "column cut";
    result = g.column_partition(total_block);
    graphsets = g.partition(result);
    graph_set<empty>::simulation(graphsets);

    return 0;
}