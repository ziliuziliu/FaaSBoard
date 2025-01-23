#include "app/cc.h"
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
    std::string graph_dir = request["graph_dir"];
    std::string result_dir = request["result_dir"];
    std::string meta_server_addr = request["meta_server"];
    std::string s3_bucket = request["s3_bucket"];
    uint32_t cores = request["cores"];
    bool no_pipeline = request["no_pipeline"];
    bool sparse_only = request["sparse_only"];
    bool dense_only = request["dense_only"];
    CAAS_SAVE_MODE save_mode = request["save_mode"];
    uint32_t request_id = request["request_id"];
    exec_config *config = new exec_config(
        graph_dir, result_dir, meta_server_addr, s3_bucket,
        no_pipeline, sparse_only, dense_only, cores, save_mode
    );
    cc(request_id, config);
    return lambda::invocation_response::success("cc success", "application/json");
}

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    FLAGS_v = 1;
    run_handler(my_handler);
    return 0;
}