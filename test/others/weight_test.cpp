#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"

#include <cstring>

int main() {
    raw_graph<uint32_t> g(41652230, 1468365182);
    g.read_txt("/data/twitter-2010.txt");
    g.save_csr("/data/twitter-2010-weighted.csr.in", "/data/twitter-2010-weighted.csr.out");
    return 0;
}