#include "app/pr.h"
#include "util/log.h"
#include "util/flags.h"
#include "util/json.h"

#include <aws/lambda-runtime/runtime.h>

using json = nlohmann::json;
namespace lambda = aws::lambda_runtime;

static lambda::invocation_response my_handler(lambda::invocation_request const& req) {
    json payload = json::parse(req.payload);
    json request = json::parse(std::string(payload["body"]));
    uint32_t request_id = request["request_id"];
    uint32_t partition_id = request["partition_id"];
    uint32_t pr_iterations = request["pr_iterations"];
    pagerank(request_id, partition_id, pr_iterations, exec_config::build_by_json(request));
    return lambda::invocation_response::success("pagerank success", "application/json");
}

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    FLAGS_v = 1;
    run_handler(my_handler);
    return 0;
}