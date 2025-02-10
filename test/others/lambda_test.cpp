#include "util/log.h"
#include "util/flags.h"
#include <aws/core/Aws.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/ListFunctionsRequest.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <iostream>

/*
 *  A "Hello Lambda" starter application which initializes an AWS Lambda (Lambda) client and lists the Lambda functions.
 *
 *  main function
 *
 *  Usage: 'hello_lambda'
 *
 */

int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    Aws::SDKOptions options;
    // Optionally change the log level for debugging.
//   options.loggingOptions.logLevel = Utils::Logging::LogLevel::Debug;
    Aws::InitAPI(options); // Should only be called once.
    int result = 0;
    {
        Aws::Client::ClientConfiguration clientConfig;
        // Optional: Set to the AWS Region (overrides config file).
        // clientConfig.region = "us-east-1";

        Aws::Lambda::LambdaClient client(clientConfig);

        VLOG(1) << "Invoking";

        Aws::Lambda::Model::InvokeRequest request;
        request.SetFunctionName("pr_d_5");
        request.SetLogType(Aws::Lambda::Model::LogType::None);
        auto payload = Aws::MakeShared<Aws::StringStream>("InputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
        std::string jsonPayload = "{\"cores\": 2, \"dense_only\": true, \"dynamic_invoke\": true, \"function_name\": \"pr_d\", \"graph_dir\": \"graph\", \"meta_server\": \"172.31.0.207\", \"no_pipeline\": false, \"partition_id\": 5, \"pr_iterations\": 17, \"request_id\": 123, \"result_dir\": \"/tmp\", \"s3_bucket\": \"ziliuziliu\", \"save_mode\": 0, \"sparse_only\": false}";
        payload -> write(jsonPayload.c_str(), jsonPayload.length());
        request.SetBody(payload);
        request.SetContentType("application/json");
        Aws::Lambda::Model::InvokeOutcome outcome = client.Invoke(request);

        if (outcome.IsSuccess()) {
            VLOG(1) << "Successfully invoked Lambda function";
        }

        else {
            VLOG(1) << "Error with Lambda::InvokeRequest. "
                      << outcome.GetError().GetMessage()
                      << std::endl;
        }
    }


    Aws::ShutdownAPI(options); // Should only be called once.
    return result;
}