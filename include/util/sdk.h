#ifndef _S3_H
#define _S3_H

#include "log.h"

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

Aws::S3::S3Client *s3_client;
Aws::Lambda::LambdaClient *lambda_client;
Aws::ECS::ECSClient *ecs_client;

void sdk_init() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    auto provider = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("alloc-tag");
    auto creds = provider -> GetAWSCredentials();
    if (creds.IsEmpty()) {
        LOG(FATAL) << "s3 failed authentication";
    }
}

void s3_sdk_init() {
    Aws::Client::ClientConfiguration client_config;
    s3_client = new Aws::S3::S3Client(client_config);
}

void lambda_sdk_init() {
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.retryStrategy = nullptr;
    lambda_client = new Aws::Lambda::LambdaClient(clientConfig);
}

void fargate_sdk_init() {
    Aws::Client::ClientConfiguration client_config;
    ecs_client = new Aws::ECS::ECSClient(client_config);
}

std::vector<std::string> s3_list_objects(std::string bucket_name) {
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

void s3_check_object_freshness(std::string bucket_name, std::string object_name) {
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

std::pair<char *, uint32_t> s3_get_object(std::string bucket_name, std::string object_name) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_name);
    Aws::S3::Model::GetObjectOutcome outcome = s3_client -> GetObject(request);
    if (!outcome.IsSuccess()) {
        const auto& error = outcome.GetError();
        LOG(FATAL) << "error getting object " << object_name
                   << " on bucket " << bucket_name
                   << " exception " << error.GetExceptionName()
                   << " message " << error.GetMessage();        
    }
    auto &data = outcome.GetResultWithOwnership().GetBody();
    auto len = outcome.GetResultWithOwnership().GetContentLength();
    char *buffer = new char[len];
    data.read(buffer, len);
    return std::make_pair(buffer, len);
}

void s3_put_object(std::string bucket_name, std::string object_name, char *data, uint32_t len) {
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

void lambda_invoke(std::string function_name, std::string payload) {
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

std::vector<std::string> fargate_get_task_ips(std::string cluster_name, std::string service_name) {
    VLOG(1) << "getting task IPs for service " << service_name << " in cluster " << cluster_name;
    std::vector<std::string> ip_addresses;
    
    // Set request parameters, only get running tasks
    Aws::ECS::Model::ListTasksRequest list_tasks_request;
    list_tasks_request.SetCluster(cluster_name);
    list_tasks_request.SetServiceName(service_name);
    list_tasks_request.SetDesiredStatus(Aws::ECS::Model::DesiredStatus::RUNNING);
    
    // Get task list
    auto list_outcome = ecs_client->ListTasks(list_tasks_request);
    if (!list_outcome.IsSuccess()) {
        const auto& error = list_outcome.GetError();
        LOG(ERROR) << "error listing tasks for service " << service_name
                   << " in cluster " << cluster_name
                   << " exception " << error.GetExceptionName()
                   << " message " << error.GetMessage();
        return ip_addresses;
    }
    
    auto task_arns = list_outcome.GetResult().GetTaskArns();
    if (task_arns.empty()) {
        LOG(WARNING) << "no running tasks found for service " << service_name;
        return ip_addresses;
    }
    
    // Describe tasks to get IP addresses
    Aws::ECS::Model::DescribeTasksRequest describe_request;
    describe_request.SetCluster(cluster_name);
    describe_request.SetTasks(task_arns);
    
    auto describe_outcome = ecs_client->DescribeTasks(describe_request);
    if (!describe_outcome.IsSuccess()) {
        const auto& error = describe_outcome.GetError();
        LOG(ERROR) << "error describing tasks in cluster " << cluster_name
                   << " exception " << error.GetExceptionName()
                   << " message " << error.GetMessage();
        return ip_addresses;
    }
    
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

#endif