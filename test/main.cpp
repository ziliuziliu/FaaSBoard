#include <aws/lambda-runtime/runtime.h>
#include <util/json.h>
#include <util/print.h>
#include <app/bfs.h>

#include <omp.h>

using json = nlohmann::json;
namespace lambda = aws::lambda_runtime;

static lambda::invocation_response my_handler(lambda::invocation_request const& req) {
    json payload = json::parse(req.payload);
    json request = json::parse(std::string(payload["body"]));
    if (request["app"] == "bfs") {
        bfs(request["request_id"], request["param"]["root"]);
    } else {
        return lambda::invocation_response::failure("Application Not Exist", "application/json");
    }
    return lambda::invocation_response::success("OK", "application/json");
}

int main() {
    run_handler(my_handler);
    return 0;
}