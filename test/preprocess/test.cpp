#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/print.h"

#include <cstring>

int main() {
    raw_graph<int> g(41652230, 1468365182);
    g.read_csr("/data/twitter-2010.csr");
    g.save_metis("/data/twitter-2010.metis");
    return 0;
}