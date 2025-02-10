#include "app/sssp.h"
#include "util/flags.h"
#include "util/types.h"

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    sssp(FLAGS_request_id, FLAGS_partition_id, FLAGS_sssp_root, exec_config::build_by_flags());
    return 0;
}