#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/print.h"

#include <cstring>

int main() {

    raw_graph<int> g(41652230, 1468365182);
    g.read_csr("/data/twitter-2010.csr");

    partition_result result;
    std::vector<graph_set<int> *> graphsets;
    int total_block = 16;

    print_log("mondriaan_row_column cut");
    result = g.mondriaan_partition_row_column(total_block);
    graphsets = g.partition(result);
    graph_set<int>::simulation(graphsets);

    return 0;
}