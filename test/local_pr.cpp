#include "app/pr.h"
#include "util/flags.h"

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    exec_config *config = new exec_config(
        FLAGS_graph_dir, FLAGS_result_dir, FLAGS_meta_server, FLAGS_s3_bucket,
        FLAGS_no_pipeline, FLAGS_sparse_only, FLAGS_dense_only, FLAGS_cores, (CAAS_SAVE_MODE)FLAGS_save_mode
    );
    pagerank(FLAGS_request_id, FLAGS_pr_iterations, config);
    return 0;
}