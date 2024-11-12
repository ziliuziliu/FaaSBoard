#include "app/pr.h"
#include "util/flags.h"

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    pagerank(FLAGS_graph_dir, FLAGS_request_id, FLAGS_no_pipeline, FLAGS_pr_iterations);
    return 0;
}