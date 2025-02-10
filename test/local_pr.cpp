#include "app/pr.h"
#include "util/flags.h"

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    pagerank(FLAGS_request_id, FLAGS_partition_id, FLAGS_pr_iterations, exec_config::build_by_flags());
    return 0;
}