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
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/Delete.h>
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
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_name);
        Aws::S3::Model::GetObjectOutcome outcome = s3_client -> GetObject(request);
        if (!outcome.IsSuccess()) {
            // const auto& error = outcome.GetError();
            // LOG(WARNING) << "Failed to get object '" << object_name
            //              << "' from bucket '" << bucket_name
            //              << "'. Exception: " << error.GetExceptionName()
            //              << ", message: " << error.GetMessage();
            return {nullptr, 0};
        }
        auto &data = outcome.GetResultWithOwnership().GetBody();
        auto len = outcome.GetResultWithOwnership().GetContentLength();
        char *buffer = new char[len];
        data.read(buffer, len);
        return std::make_pair(buffer, len);
    }
    
    void put_object(std::string bucket_name, std::string object_name, char* data, uint32_t len) {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_name);

        // Create a string stream to hold the data
        auto body = Aws::MakeShared<Aws::StringStream>(
            "InputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary
        );
        body->write(data, len);
        request.SetBody(body);


        // Execute the PutObject operation
        auto outcome = s3_client->PutObject(request);
        if (!outcome.IsSuccess()) {
            const auto& error = outcome.GetError();
            LOG(FATAL) << "error putting object " << object_name
                       << " on bucket " << bucket_name
                       << " exception " << error.GetExceptionName()
                       << " message " << error.GetMessage();
        }
        // } else {
        //     LOG(INFO) << "Successfully put object: " << object_name << " to bucket: " << bucket_name;
        // }
    }

    void delete_all_objects(const std::string& bucket_name) {
        auto objects = list_objects(bucket_name);
        for (const auto& key : objects) {
            Aws::S3::Model::DeleteObjectRequest request;
            request.SetBucket(bucket_name);
            request.SetKey(key);
    
            auto outcome = s3_client->DeleteObject(request);
            if (!outcome.IsSuccess()) {
                const auto& error = outcome.GetError();
                LOG(FATAL) << "error deleting object " << key
                             << " exception " << error.GetExceptionName()
                             << " message " << error.GetMessage();
            }
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
};

#endif