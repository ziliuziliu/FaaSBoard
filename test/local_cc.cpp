#include "app/cc.h" 
#include "util/flags.h"
#include "util/types.h"

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    cc(FLAGS_request_id, FLAGS_partition_id, exec_config::build_by_flags());
    VLOG(1) << "Completed CC computation"; 
    return 0;
}
