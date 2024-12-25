#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/types.h"
#include "util/log.h"
#include "util/flags.h"

#include <cstring>

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    raw_graph<empty> g(FLAGS_vertices, FLAGS_edges * (FLAGS_undirected ? 2 : 1));
    g.read_txt(FLAGS_graph_file, FLAGS_undirected);
    g.save_csr(FLAGS_graph_file + ".csr.in", FLAGS_graph_file + ".csr.out");
    return 0;
}