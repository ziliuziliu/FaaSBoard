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
    int total_block = FLAGS_partitions, cut = FLAGS_partitions;
    VLOG(1) << "stagger cut";
    result = g.naive_checkerboard_partition(cut);
    VLOG(1) << "begin partitioning";
    graphsets = g.partition(result);
    VLOG(1) << "stagger placing";
    graphsets = graph_set<empty>::stagger(graphsets, total_block);
    graph_set<empty>::simulation(graphsets);
    return 0;
}