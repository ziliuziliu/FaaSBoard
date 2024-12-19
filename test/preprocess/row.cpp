#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/log.h"
#include "util/types.h"

#include <cstring>

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    raw_graph<empty> g(FLAGS_vertices, FLAGS_edges);
    g.read_csr(FLAGS_graph_file + ".csr.in", FLAGS_graph_file + ".csr.out");
    partition_result result;
    std::vector<graph_set<empty> *> graphsets;
    int total_block = FLAGS_partitions;
    VLOG(1) << "row cut";
    result = g.row_partition(total_block);
    graphsets = g.partition(result);
    graph_set<empty>::simulation(graphsets);
    return 0;
}