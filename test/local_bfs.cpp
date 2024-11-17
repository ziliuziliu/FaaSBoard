#include "app/bfs.h"
#include "util/flags.h"
#include "util/types.h"

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    exec_config *config = new exec_config(
        FLAGS_graph_dir, FLAGS_meta_server, FLAGS_no_pipeline, FLAGS_sparse_only, FLAGS_dense_only,
        FLAGS_cores, FLAGS_save_mode
    );
    bfs(FLAGS_request_id, FLAGS_bfs_root, config);
    return 0;
}