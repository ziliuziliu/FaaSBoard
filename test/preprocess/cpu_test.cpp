#include <aws/lambda-runtime/runtime.h>
#include <util/json.h>
#include <util/print.h>
#include <omp.h>

using json = nlohmann::json;
namespace lambda = aws::lambda_runtime;

static lambda::invocation_response my_handler(lambda::invocation_request const& req) {
    json payload = json::parse(req.payload);
    json request = json::parse(std::string(payload["body"]));
    int upper_limit = request["top"];
    auto before = std::chrono::high_resolution_clock::now();
    int total = 0;
    #pragma omp parallel for reduction(+:total)
    for (int i = 2; i <= upper_limit; i++) {
        int top = sqrt(i);
        bool divided = false;
        for (int j = 2; j <= top; j++) {
            if (i % j == 0) {
                divided = true;
                break;
            }
        }
        total += !divided;
    }
    auto after = std::chrono::high_resolution_clock::now();
    auto interval = std::chrono::duration_cast<std::chrono::nanoseconds>(after - before).count();
    json response;
    response["interval"] = interval / 1e9;
    response["prime_numbers"] = total;
    return lambda::invocation_response::success(response.dump(), "application/json");
}

int main() {
    run_handler(my_handler);
    return 0;
}