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
    int total_block = 16, cut;

    print_log("naive cut + stagger layout");
    cut = total_block;
    result = g.naive_checkerboard_partition(cut);
    print_log("begin partitioning");
    graphsets = g.partition(result);
    print_log("stagger placing");
    graphsets = graph_set<int>::stagger(graphsets, total_block);
    graph_set<int>::simulation(graphsets);

    return 0;
}