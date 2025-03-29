#ifndef _S3_H
#define _S3_H

#include "log.h"

#include <hiredis/hiredis.h>
#include <hiredis/hiredis_ssl.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/Object.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/ecs/ECSClient.h>
#include <aws/ecs/model/ListTasksRequest.h>
#include <aws/ecs/model/DescribeTasksRequest.h>
#include <vector>
#include <chrono>
#include <fstream>

void sdk_init() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    auto provider = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("credentials_provider");
    auto creds = provider -> GetAWSCredentials();
    if (creds.IsEmpty()) {
        LOG(FATAL) << "can't find aws credentials";
    }
}

struct s3_sdk {

    Aws::UniquePtr<Aws::S3::S3Client> s3_client;

    s3_sdk() {
        Aws::Client::ClientConfiguration client_config;
        s3_client = Aws::MakeUnique<Aws::S3::S3Client>("s3_client", client_config);
    }

    std::vector<std::string> list_objects(std::string bucket_name) {
        Aws::S3::Model::ListObjectsV2Request request;
        request.WithBucket(bucket_name);
        Aws::String continuation_token;
        Aws::Vector<Aws::S3::Model::Object> all_objects;
        do {
            if (!continuation_token.empty()) {
                request.SetContinuationToken(continuation_token);
            }
            auto outcome = s3_client -> ListObjectsV2(request);
            if (!outcome.IsSuccess()) {
                const auto& error = outcome.GetError();
                LOG(FATAL) << "error listing objects from bucket " << bucket_name
                           << " exception " << error.GetExceptionName()
                           << " message " << error.GetMessage();
            }
            Aws::Vector<Aws::S3::Model::Object> objects = outcome.GetResult().GetContents();
            all_objects.insert(all_objects.end(), objects.begin(), objects.end());
            continuation_token = outcome.GetResult().GetNextContinuationToken();
        } while (!continuation_token.empty());
        std::vector<std::string> object_list;
        for (const auto &object : all_objects) {
            object_list.push_back(object.GetKey());
        }
        return object_list;
    }
    
    void check_object_freshness(std::string bucket_name, std::string object_name) {
        Aws::S3::Model::HeadObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_name);
        auto outcome = s3_client -> HeadObject(request);
        if (!outcome.IsSuccess()) {
            const auto& error = outcome.GetError();
            LOG(FATAL) << "error checking freshness for object " << object_name
                       << " on bucket " << bucket_name
                       << " exception " << error.GetExceptionName()
                       << " message " << error.GetMessage();
        }
        auto last_modified = outcome.GetResult().GetLastModified();
        auto current = Aws::Utils::DateTime::Now();
        auto diff_time = std::chrono::duration_cast<std::chrono::minutes>(current - last_modified).count();
        CHECK(diff_time <= 30) << "result file is generated long time ago";
    }
    
    std::pair<char *, uint32_t> get_object(std::string bucket_name, std::string object_name) {
        VLOG(1) << "set request for getting object " << object_name << " from bucket " << bucket_name;
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_name);

        VLOG(1) << "get object";
        Aws::S3::Model::GetObjectOutcome outcome = s3_client -> GetObject(request);
        if (!outcome.IsSuccess()) {
            const auto& error = outcome.GetError();
            LOG(FATAL) << "error getting object " << object_name
                       << " on bucket " << bucket_name
                       << " exception " << error.GetExceptionName()
                       << " message " << error.GetMessage();        
        }

        VLOG(1) << "handle object data";
        auto &data = outcome.GetResultWithOwnership().GetBody();
        VLOG(1) << "get object length";
        auto len = outcome.GetResultWithOwnership().GetContentLength();
        VLOG(1) << "allocate buffer";
        char *buffer = new char[len];
        VLOG(1) << "read object data";
        data.read(buffer, len);
        VLOG(1) << "return object data";
        return std::make_pair(buffer, len);
    }
    
    void put_object(std::string bucket_name, std::string object_name, char *data, uint32_t len) {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_name);
        auto body = Aws::MakeShared<Aws::StringStream>(
            "InputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary
        );
        body -> write(data, len);
        request.SetBody(body);
        auto outcome = s3_client -> PutObject(request);
        if (!outcome.IsSuccess()) {
            const auto& error = outcome.GetError();
            LOG(FATAL) << "error putting object " << object_name
                       << " on bucket " << bucket_name
                       << " exception " << error.GetExceptionName()
                       << " message " << error.GetMessage();
        }
    }

};

struct lambda_sdk {

    Aws::UniquePtr<Aws::Lambda::LambdaClient> lambda_client;

    lambda_sdk() {
        Aws::Client::ClientConfiguration client_config;
        client_config.retryStrategy = nullptr;
        lambda_client = Aws::MakeUnique<Aws::Lambda::LambdaClient>("lambda_client", client_config);
    }

    void invoke(std::string function_name, std::string payload) {
        VLOG(1) << "invoking lambda function " << function_name << " with payload " << payload;
        Aws::Lambda::Model::InvokeRequest invoke_request;
        invoke_request.SetFunctionName(function_name);
        invoke_request.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
        invoke_request.SetLogType(Aws::Lambda::Model::LogType::None);
        auto body = Aws::MakeShared<Aws::StringStream>(
            "InputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary
        );
        body -> write(payload.c_str(), payload.length());
        invoke_request.SetBody(body);
        auto outcome = lambda_client -> Invoke(invoke_request);
        if (!outcome.IsSuccess()) {
            const auto& error = outcome.GetError();
            LOG(FATAL) << "error invoking lambda function " << function_name
                       << " exception " << error.GetExceptionName()
                       << " message " << error.GetMessage();
        }
        auto &result = outcome.GetResult();
        Aws::IOStream &payload_stream = result.GetPayload();
        std::string payload_str;
        std::getline(payload_stream, payload_str);
        LOG(INFO) << "lambda function " << function_name << " result: " << payload_str;
    }

};

struct fargate_sdk {

    Aws::UniquePtr<Aws::ECS::ECSClient> ecs_client;

    fargate_sdk() {
        Aws::Client::ClientConfiguration client_config;
        ecs_client = Aws::MakeUnique<Aws::ECS::ECSClient>("ecs_client", client_config);
    }

    std::vector<std::string> get_task_ips(std::string cluster_name, std::string service_name) {
        VLOG(1) << "getting task IPs for service " << service_name << " in cluster " << cluster_name;
        std::vector<std::string> ip_addresses;
        
        // Set request parameters, only get running tasks
        Aws::ECS::Model::ListTasksRequest list_tasks_request;
        list_tasks_request.SetCluster(Aws::String(cluster_name));
        list_tasks_request.SetServiceName(Aws::String(service_name));
        list_tasks_request.SetDesiredStatus(Aws::ECS::Model::DesiredStatus::RUNNING);
        
        VLOG(1) << "get task list";
        // Get task list
        auto list_outcome = ecs_client -> ListTasks(list_tasks_request);
        if (!list_outcome.IsSuccess()) {
            const auto& error = list_outcome.GetError();
            LOG(ERROR) << "error listing tasks for service " << service_name
                       << " in cluster " << cluster_name
                       << " exception " << error.GetExceptionName()
                       << " message " << error.GetMessage();
            return ip_addresses;
        }
        
        VLOG(1) << "get task ARNs";
        auto task_arns = list_outcome.GetResult().GetTaskArns();
        if (task_arns.empty()) {
            LOG(WARNING) << "no running tasks found for service " << service_name;
            return ip_addresses;
        }
        
        // Describe tasks to get IP addresses
        Aws::ECS::Model::DescribeTasksRequest describe_request;
        describe_request.SetCluster(Aws::String(cluster_name));
        describe_request.SetTasks(task_arns);
        
        auto describe_outcome = ecs_client -> DescribeTasks(describe_request);
        if (!describe_outcome.IsSuccess()) {
            const auto& error = describe_outcome.GetError();
            LOG(ERROR) << "error describing tasks in cluster " << cluster_name
                       << " exception " << error.GetExceptionName()
                       << " message " << error.GetMessage();
            return ip_addresses;
        }
        
        VLOG(1) << "get task IP addresses";
        // Get IP addresses from task attachments
        for (const auto& task : describe_outcome.GetResult().GetTasks()) {
            if (task.GetLastStatus() == "RUNNING") {
                for (const auto& attachment : task.GetAttachments()) {
                    if (attachment.GetType() == "ElasticNetworkInterface") {
                        for (const auto& detail : attachment.GetDetails()) {
                            if (detail.GetName() == "privateIPv4Address") {
                                ip_addresses.push_back(detail.GetValue());
                                VLOG(1) << "found task private IP: " << detail.GetValue();
                            }
                        }
                    }
                }
            }
        }
        
        LOG(INFO) << "found " << ip_addresses.size() << " IP addresses for service " << service_name;
        return ip_addresses;
    }

};

class elasticache_sdk {

    private:

        redisContext* redisCtx;
        std::string host;
        int port;
        bool isCluster;
    
    public:
    
        elasticache_sdk() {}
    
        elasticache_sdk(const std::string& _host, int _port) 
            : redisCtx(nullptr)
            , host(_host)
            , port(_port)
            , isCluster(false) {
        }
    
        ~elasticache_sdk() {
            if (redisCtx) {
                redisFree(redisCtx);
            }
        }
    
        bool connect() {
            // Connect to Redis
            redisCtx = redisConnect(host.c_str(), port);
            if (redisCtx == nullptr || redisCtx->err) {
                std::cerr << "Failed to connect to Redis: "
                          << (redisCtx ? redisCtx->errstr : "unknown error") << std::endl;
                return false;
            }
    
            // Set up SSL/TLS
            redisSSLContext* sslContext = redisCreateSSLContext(
                "cacert.pem",  // cert file
                nullptr,  // key file
                nullptr,  // ca cert
                nullptr,  // ca path
                nullptr,  // server name
                nullptr   // error
            );
    
            if (!sslContext) {
                std::cerr << "Failed to create SSL context" << std::endl;
                redisFree(redisCtx);
                redisCtx = nullptr;
                return false;
            }
    
            if (redisInitiateSSLWithContext(redisCtx, sslContext) != REDIS_OK) {
                std::cerr << "Failed to establish SSL connection: " << redisCtx->errstr << std::endl;
                redisFree(redisCtx);
                redisCtx = nullptr;
                redisFreeSSLContext(sslContext);
                return false;
            }
    
            redisFreeSSLContext(sslContext);
    
            // Check if it's a cluster
            redisReply* reply = (redisReply*)redisCommand(redisCtx, "CLUSTER INFO");
            if (reply && reply->type != REDIS_REPLY_ERROR) {
                isCluster = true;
            }
            if (reply) freeReplyObject(reply);
    
            return true;
        }
    
        bool setnx(const std::string& key, const std::string& value) {
            if (!redisCtx) {
                std::cerr << "Redis context is not initialized." << std::endl;
                return false;
            }
    
            redisReply* reply = (redisReply*)redisCommand(redisCtx, "SETNX %s %s", 
                                                        key.c_str(), value.c_str());
            if (!reply) {
                std::cerr << "SETNX command failed." << std::endl;
                return false;
            }
    
            // Redis returns 1 if the key was set, 0 if it already exists
            bool success = (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
            
            freeReplyObject(reply);
            return success;
        }
    
        bool setList(const std::string& key, const std::vector<std::string>& values) {
            if (!redisCtx) {
                std::cerr << "Redis context is not initialized." << std::endl;
                return false;
            }
    
            redisReply* reply = (redisReply*)redisCommand(redisCtx, "DEL %s", key.c_str());
            if (!reply) {
                std::cerr << "DEL command failed." << std::endl;
                return false;
            }
            freeReplyObject(reply);
    
            for (const auto& value : values) {
                reply = (redisReply*)redisCommand(redisCtx, "RPUSH %s %s", key.c_str(), value.c_str());
                if (!reply) {
                    std::cerr << "RPUSH command failed." << std::endl;
                    return false;
                }
                freeReplyObject(reply);
            }
    
            return true;
        }
    
        std::string get(const std::string& key) {
            if (!redisCtx) {
                std::cerr << "Redis context is not initialized." << std::endl;
                return "";
            }
    
            redisReply* reply = (redisReply*)redisCommand(redisCtx, "GET %s", key.c_str());
            if (!reply || reply->type != REDIS_REPLY_STRING) {
                std::cerr << "GET command failed or key not found." << std::endl;
                if (reply) freeReplyObject(reply);
                return "";
            }
    
            std::string value = reply->str;
            freeReplyObject(reply);
            return value;
        }
    
        std::vector<std::string> getList(const std::string& key) {
            std::vector<std::string> values;
    
            if (!redisCtx) {
                std::cerr << "Redis context is not initialized." << std::endl;
                return values;
            }
    
            redisReply* reply = (redisReply*)redisCommand(redisCtx, "LRANGE %s 0 -1", key.c_str());
            if (!reply || reply->type != REDIS_REPLY_ARRAY) {
                std::cerr << "LRANGE command failed or key not found." << std::endl;
                if (reply) freeReplyObject(reply);
                return values;
            }
    
            for (size_t i = 0; i < reply->elements; i++) {
                values.push_back(reply->element[i]->str);
            }
    
            freeReplyObject(reply);
            return values;
        }
    
        void close() {
            if (redisCtx) {
                redisFree(redisCtx);
                redisCtx = nullptr;
            }
        }

        template<typename T>
        size_t get_object_to_buffer_auto(const std::string& relative_path, 
                                        T* buffer, size_t element_count, size_t element_size = sizeof(T)) 
        {
            if (!buffer) {
                LOG(FATAL) << "Buffer is null";
            }

            VLOG(1) << "Start reading object from ElastiCache faasboard:{" + relative_path + "}";

            std::string base_key = "faasboard:{" + relative_path + "}";
            size_t expected_bytes = element_count * element_size;
            size_t bytes_read = 0;

            VLOG(1) << "Try single flie mode";

            // Step 1: Try GET first
            redisReply* reply = (redisReply*)redisCommand(redisCtx, "GET %s", base_key.c_str());

            if (reply && reply->type == REDIS_REPLY_STRING) {
                // Single file path
                size_t len = reply->len;
                VLOG(1) << "Single file mode: read " << len << " bytes";
                memcpy(reinterpret_cast<char*>(buffer), reply->str, len);
                bytes_read = len;

                VLOG(1) << "Single file finished";

                freeReplyObject(reply);
                return bytes_read / element_size;
            }

            if (reply) freeReplyObject(reply); // Always cleanup

            // Step 2: Fallback to chunked mode
            std::string hash_key = base_key + ":chunks";
            
            VLOG(1) << "Try chunked mode";

            // Get total_chunks
            reply = (redisReply*)redisCommand(redisCtx, "HGET %s %s", hash_key.c_str(), "total_chunks");
            if (!reply || reply->type != REDIS_REPLY_STRING) {
                LOG(FATAL) << "Neither GET nor HGET total_chunks succeeded for key: " << relative_path;
            }

            VLOG(1) << "Chunked mode: total chunks " << reply->str;

            int total_chunks = std::stoi(reply->str);
            freeReplyObject(reply);

            VLOG(1) << "Start reading chunks";

            // Sequentially read chunks
            for (int i = 0; i < total_chunks; ++i) {
                std::string chunk_field = "chunk:" + std::to_string(i);

                VLOG(1) << "Reading chunk " << chunk_field;

                reply = (redisReply*)redisCommand(redisCtx, "HGET %s %s", 
                                                hash_key.c_str(), chunk_field.c_str());

                VLOG(1) << "Chunk read finish ";

                if (!reply || reply->type != REDIS_REPLY_STRING) {
                    LOG(FATAL) << "Missing or invalid chunk: " << chunk_field;
                }

                size_t chunk_size = reply->len;
                if (bytes_read + chunk_size > expected_bytes) {
                    freeReplyObject(reply);
                    LOG(FATAL) << "Chunk read exceeds buffer size";
                }

                VLOG(1) << "Chunked mode: read " << chunk_size << " bytes";

                memcpy(reinterpret_cast<char*>(buffer) + bytes_read, reply->str, chunk_size);
                bytes_read += chunk_size;

                VLOG(1) << "Chunked mode: total read " << bytes_read << " bytes";

                freeReplyObject(reply);
            }

            CHECK(bytes_read == expected_bytes) 
                << "Chunked read mismatch: expected " << expected_bytes << ", got " << bytes_read;

            return bytes_read / element_size;
        }
};

class elasticache_cluster_sdk {

private:
    redisContext* redisCtx;
    std::vector<std::pair<std::string, int>> nodes;
    std::string currentHost;
    int currentPort;
    
public:
    // 构造函数
    elasticache_cluster_sdk(const std::vector<std::pair<std::string, int>>& _nodes) 
        : redisCtx(nullptr), nodes(_nodes) {
        if (!nodes.empty()) {
            currentHost = nodes[0].first;
            currentPort = nodes[0].second;
        }
    }
    
    ~elasticache_cluster_sdk() {
        close();
    }
    
    // 连接函数
    bool connect() {
        if (redisCtx) {
            redisFree(redisCtx);
            redisCtx = nullptr;
        }
        
        redisCtx = redisConnect(currentHost.c_str(), currentPort);
        if (redisCtx == nullptr || redisCtx->err) {
            if (redisCtx) redisFree(redisCtx);
            redisCtx = nullptr;
            return false;
        }
        
        redisSSLContext* sslContext = redisCreateSSLContext(
            "cacert.pem", nullptr, nullptr, nullptr, nullptr, nullptr
        );
        
        if (!sslContext || redisInitiateSSLWithContext(redisCtx, sslContext) != REDIS_OK) {
            if (redisCtx) redisFree(redisCtx);
            if (sslContext) redisFreeSSLContext(sslContext);
            redisCtx = nullptr;
            return false;
        }
        
        redisFreeSSLContext(sslContext);
        return true;
    }
    
    // 关闭连接
    void close() {
        if (redisCtx) {
            redisFree(redisCtx);
            redisCtx = nullptr;
        }
    }
    
    // 从集群读取对象到缓冲区
    template<typename T>
    size_t get_object_to_buffer_auto(const std::string& relative_path, 
                                    T* buffer, size_t element_count, size_t element_size = sizeof(T)) 
    {
        if (!buffer) {
            LOG(FATAL) << "Buffer is null";
        }
        
        size_t expected_bytes = element_count * element_size;
        LOG(INFO) << "Attempting to read " << expected_bytes << " bytes from key: " << relative_path;
        
        // 遍历所有节点尝试读取
        for (size_t node_idx = 0; node_idx < nodes.size(); ++node_idx) {
            currentHost = nodes[node_idx].first;
            currentPort = nodes[node_idx].second;
            
            LOG(INFO) << "Trying node " << currentHost << ":" << currentPort;
            
            // 连接到当前节点
            if (!connect()) {
                LOG(INFO) << "Failed to connect to node " << currentHost << ":" << currentPort;
                continue;
            }
            
            std::string base_key = "faasboard:{" + relative_path + "}";
            size_t bytes_read = 0;
            
            // 1. 尝试单文件模式
            LOG(INFO) << "Trying single file mode with key: " << base_key;
            redisReply* reply = (redisReply*)redisCommand(redisCtx, "GET %s", base_key.c_str());
            
            if (reply && reply->type == REDIS_REPLY_STRING) {
                LOG(INFO) << "Single file mode successful, read " << reply->len << " bytes";
                memcpy(reinterpret_cast<char*>(buffer), reply->str, reply->len);
                bytes_read = reply->len;
                freeReplyObject(reply);
                return bytes_read / element_size;
            }
            
            if (reply) {
                LOG(INFO) << "Single file mode failed, reply type: " << reply->type;
                freeReplyObject(reply);
            } else {
                LOG(INFO) << "Single file mode failed, no reply";
            }
            
            // 2. 尝试分块模式
            std::string hash_key = base_key + ":chunks";
            LOG(INFO) << "Trying chunked mode with key: " << hash_key;
            
            // 检查哈希表是否存在
            reply = (redisReply*)redisCommand(redisCtx, "EXISTS %s", hash_key.c_str());
            if (!reply || reply->type != REDIS_REPLY_INTEGER || reply->integer == 0) {
                LOG(INFO) << "Hash key does not exist: " << hash_key;
                if (reply) freeReplyObject(reply);
                continue;
            }
            LOG(INFO) << "Hash key exists: " << hash_key;
            if (reply) freeReplyObject(reply);
            
            // 获取总块数
            reply = (redisReply*)redisCommand(redisCtx, "HGET %s %s", hash_key.c_str(), "total_chunks");
            if (!reply || reply->type != REDIS_REPLY_STRING) {
                LOG(INFO) << "Failed to get total_chunks from hash key";
                if (reply) freeReplyObject(reply);
                
                // 尝试列出哈希表中所有的字段，查看是否有可能用不同的字段名存储
                LOG(INFO) << "Trying to list all fields in hash";
                redisReply* fields_reply = (redisReply*)redisCommand(redisCtx, "HKEYS %s", hash_key.c_str());
                if (fields_reply && fields_reply->type == REDIS_REPLY_ARRAY) {
                    LOG(INFO) << "Fields in hash (" << fields_reply->elements << " total):";
                    for (size_t i = 0; i < std::min(fields_reply->elements, (size_t)10); i++) {
                        LOG(INFO) << "  " << fields_reply->element[i]->str;
                    }
                }
                if (fields_reply) freeReplyObject(fields_reply);
                continue;
            }
            
            int total_chunks = std::stoi(reply->str);
            LOG(INFO) << "Total chunks: " << total_chunks;
            freeReplyObject(reply);
            
            bool success = true;
            
            // 读取所有块
            for (int i = 0; i < total_chunks; ++i) {
                std::string chunk_field = "chunk:" + std::to_string(i);
                LOG(INFO) << "Reading chunk " << i+1 << "/" << total_chunks << ": " << chunk_field;
                
                reply = (redisReply*)redisCommand(redisCtx, "HGET %s %s", hash_key.c_str(), chunk_field.c_str());
                
                if (!reply || reply->type != REDIS_REPLY_STRING) {
                    LOG(INFO) << "Failed to read chunk " << i;
                    if (reply) freeReplyObject(reply);
                    success = false;
                    break;
                }
                
                if (bytes_read + reply->len > expected_bytes) {
                    LOG(INFO) << "Chunk read would exceed buffer size: " << bytes_read << " + " << reply->len << " > " << expected_bytes;
                    freeReplyObject(reply);
                    success = false;
                    break;
                }
                
                LOG(INFO) << "Read chunk " << i << ": " << reply->len << " bytes";
                memcpy(reinterpret_cast<char*>(buffer) + bytes_read, reply->str, reply->len);
                bytes_read += reply->len;
                LOG(INFO) << "Total bytes read so far: " << bytes_read << "/" << expected_bytes;
                freeReplyObject(reply);
            }
            
            if (success) {
                if (bytes_read == expected_bytes) {
                    LOG(INFO) << "Successfully read all " << bytes_read << " bytes";
                    return bytes_read / element_size;
                } else {
                    LOG(INFO) << "Bytes read (" << bytes_read << ") doesn't match expected bytes (" << expected_bytes << ")";
                }
            }
        }
        
        // 所有节点都尝试失败
        LOG(FATAL) << "Failed to read data from any node for key: " << relative_path;
        return 0;
    }
};

#endif