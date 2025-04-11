#include "util/sdk.h"
#include <aws/core/Aws.h>
#include <iostream>
#include <thread>
#include <vector>
#include <cstring>
#include <chrono>
#include <sstream>
#include <random>
#include <getopt.h>


const std::string TEST_BUCKET = "ykruan";

// 数据生成函数：构造 (3 * uint32_t) + bit流
std::pair<char*, size_t> generate_test_data(uint32_t a, uint32_t b, uint32_t c, size_t bitstream_size) {
    size_t total_size = 12 + bitstream_size;
    char* buffer = new char[total_size];

    std::memcpy(buffer, &a, 4);
    std::memcpy(buffer + 4, &b, 4);
    std::memcpy(buffer + 8, &c, 4);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);

    for (size_t i = 0; i < bitstream_size; ++i) {
        buffer[12 + i] = static_cast<char>(dis(gen));
    }

    return {buffer, total_size};
}

// 检查下载对象是否与上传匹配
bool validate_test_data(char* data, size_t size, uint32_t a, uint32_t b, uint32_t c, size_t bitstream_size) {
    if (size != 12 + bitstream_size) return false;

    uint32_t a_, b_, c_;
    std::memcpy(&a_, data, 4);
    std::memcpy(&b_, data + 4, 4);
    std::memcpy(&c_, data + 8, 4);

    if (a_ != a || b_ != b || c_ != c) return false;

    return true;  // bitstream 不比对值，只验证长度与 header
}

void print_test_result(const std::string& test_name, bool passed, int thread_id) {
    std::cout << "[Thread " << thread_id << "] Test: " << test_name << " - "
              << (passed ? "PASSED" : "FAILED") << std::endl;
}

bool test_object_io(s3_sdk* s3, const std::string& object_key, int thread_id) {
    uint32_t a = thread_id + 1;
    uint32_t b = thread_id + 10;
    uint32_t c = thread_id + 100;
    size_t bitstream_size = 128;  // 模拟 bitstream 部分长度

    auto [data, data_len] = generate_test_data(a, b, c, bitstream_size);

    s3->put_object(TEST_BUCKET, object_key, data, data_len);

    auto result = s3->get_object(TEST_BUCKET, object_key);
    bool valid = validate_test_data(result.first, result.second, a, b, c, bitstream_size);

    delete[] data;
    delete[] result.first;
    return valid;
}

void thread_worker(int thread_id) {
    std::ostringstream object_name;
    object_name << "thread_data_object_" << thread_id;

    s3_sdk* s3 = new s3_sdk();

    bool ok = test_object_io(s3, object_name.str(), thread_id);
    print_test_result("Object IO with header + bitstream", ok, thread_id);

    delete s3;
}

int main(int argc, char* argv[]) {
    int thread_base = 0;
    int num_threads = 4;

    // 参数解析：支持 -thread_base_id 和 -cores
    const struct option long_options[] = {
        {"thread_base_id", required_argument, 0, 't'},
        {"cores", required_argument, 0, 'c'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "t:c:", long_options, nullptr)) != -1) {
        switch (opt) {
            case 't': thread_base = std::stoi(optarg); break;
            case 'c': num_threads = std::stoi(optarg); break;
            default:
                std::cerr << "Usage: " << argv[0]
                          << " --thread_base_id <id> --cores <num>" << std::endl;
                return 1;
        }
    }

    std::cout << "Starting multi-threaded S3 SDK tests with thread_base_id = "
              << thread_base << ", cores = " << num_threads << std::endl;

    sdk_init();

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        int thread_id = thread_base + i;
        threads.emplace_back(thread_worker, thread_id);
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "All threads completed." << std::endl;
    return 0;
}