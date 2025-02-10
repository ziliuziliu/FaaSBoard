#include "app/bfs.h"
#include "util/log.h"
#include "util/flags.h"
#include "util/json.h"

#include <aws/lambda-runtime/runtime.h>

using json = nlohmann::json;
namespace lambda = aws::lambda_runtime;

static lambda::invocation_response my_handler(lambda::invocation_request const& req) {
    VLOG(1) << "payload " << req.payload;
    if (req.payload == "\"ping\"") {
        return lambda::invocation_response::success("pong", "application/json");
    }
    json request = json::parse(req.payload);
    uint32_t request_id = request["request_id"];
    uint32_t partition_id = request["partition_id"];
    uint32_t bfs_root = request["bfs_root"];
    bfs(request_id, partition_id, bfs_root, exec_config::build_by_json(request));
    return lambda::invocation_response::success("bfs success", "application/json");
}

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    FLAGS_v = 1;
    run_handler(my_handler);
    return 0;
}