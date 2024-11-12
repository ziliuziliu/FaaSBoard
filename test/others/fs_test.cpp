#include <aws/lambda-runtime/runtime.h>
#include <util/json.h>
#include <filesystem>
#include <iostream>
#include <unistd.h>
#include <limits.h>

using json = nlohmann::json;
namespace lambda = aws::lambda_runtime;
namespace fs = std::filesystem;

void traverse_directory(const fs::path& dir_path, std::string& result, int level = 0) {
    // Stop traversal after 3 levels
    if (level > 2) return;

    // Add the full path of the directory
    result += std::string(level * 2, ' ') + dir_path.string() + "\n";

    try {
        // Iterate over all entries in the directory
        for (const auto& entry : fs::directory_iterator(dir_path)) {
            if (entry.is_directory()) {
                traverse_directory(entry.path(), result, level + 1);
            }
        }
    } catch (const fs::filesystem_error& e) {
        // Skip directories that can't be accessed due to permissions
        result += std::string(level * 2, ' ') + "  [Permission Denied]\n";
    }
}

static lambda::invocation_response my_handler(lambda::invocation_request const& req) {
    std::cout << req.payload << std::endl;
    std::string result;
    fs::path root = fs::current_path().root_path();
    traverse_directory(root, result);
    json response;
    response["cwd"] = result;
    return lambda::invocation_response::success(response.dump(), "application/json");
}

int main() {
    run_handler(my_handler);
    return 0;
}