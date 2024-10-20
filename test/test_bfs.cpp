#include "app/bfs.h"
#include "util/flags.h"

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    bfs(FLAGS_graph_dir, FLAGS_request_id, FLAGS_no_pipeline, FLAGS_bfs_root);
    return 0;
}