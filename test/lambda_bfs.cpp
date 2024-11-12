#include "app/bfs.h"
#include "util/json.h"

#include <aws/lambda-runtime/runtime.h>

using json = nlohmann::json;
namespace lambda = aws::lambda_runtime;

static lambda::invocation_response my_handler(lambda::invocation_request const& req) {
    json payload = json::parse(req.payload);
    json request = json::parse(std::string(payload["body"]));
    std::string graph_dir = request["graph_dir"];
    std::string meta_server_addr = request["meta_server"];
    uint32_t cores = request["cores"];
    bool no_pipeline = request["no_pipeline"];
    bool sparse_only = request["sparse_only"];
    bool dense_only = request["dense_only"];
    int save_mode = request["save_mode"];
    uint32_t request_id = request["request_id"];
    uint32_t bfs_root = request["bfs_root"];
    exec_config *config = new exec_config(
        graph_dir, meta_server_addr, no_pipeline, sparse_only, dense_only, cores, save_mode
    );
    bfs(request_id, bfs_root, config);
    return lambda::invocation_response::success("bfs success", "application/json");
}

int main() {
    run_handler(my_handler);
    return 0;
}