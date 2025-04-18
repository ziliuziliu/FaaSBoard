#ifndef _FLAGS_H
#define _FLAGS_H

#include "util/types.h"
#include "util/log.h"
#include "util/json.h"

#include <gflags/gflags.h>
#include <vector>
#include <iostream>
#include <sstream>

using json = nlohmann::json;

DEFINE_string(graph_root_dir, ".", "root directory for graph dataset in csr binary");
DEFINE_string(graph_file, "", "original graph dataset file");
DEFINE_uint32(vertices, 0, "#vertices");
DEFINE_uint64(edges, 0, "#edges");
DEFINE_uint32(request_id, 0, "request id");
DEFINE_uint32(bfs_root, 0xffffffff, "root vertex for bfs");
DEFINE_uint32(sssp_root, 0xffffffff, "root vertex for sssp");
DEFINE_int32(pr_iterations, -1, "iterations for pagerank");
DEFINE_string(graph_dir, ".", "directory for graph dataset in csr binary");
DEFINE_string(result_dir, ".", "directory for compute result");
DEFINE_string(meta_server, "", "meta server address (ip)");
DEFINE_string(proxy_server_list, "", "list of available proxy server addresses (ip) separated by comma");
DEFINE_uint32(cores, 0, "cores to use");
DEFINE_string(application, "", "application type (bfs, cc, pr, sssp)");
DEFINE_bool(no_pipeline, false, "no in-exec_each-out-exec_diagonal pipeline");
DEFINE_int32(partitions, 0, "how many functions to hold the graph");
DEFINE_bool(sparse_only, false, "sparse mode");
DEFINE_bool(dense_only, false, "dense mode");
DEFINE_bool(need_global_degree, false, "need to read global degree");
DEFINE_int32(save_mode, 1, "result save mode (0: no save, 1: local disk, 2: s3)");
DEFINE_string(s3_bucket, "", "s3 bucket name");
DEFINE_bool(undirected, false, "whether it's undirected graph (0: directed, 1: undirected)");
DEFINE_string(ewT, "empty", "Whether to specify the edge weight type (empty,int,uint32_t,float)");
DEFINE_string(strategy, "checkerboard", "partitioning strategy: row, column, mondriaan_row_column, mondriaan_column, cycle, stagger, checkerboard");
DEFINE_bool(dynamic_invoke, false, "enable dynamic invoke");
DEFINE_uint32(partition_id, 0xffffffff, "partition id (0-based)");
DEFINE_int32(kill_wait_ms, 100000, "wait time to kill process");
DEFINE_bool(elastic_proxy, false, "enable elastic proxy");
DEFINE_string(elasticache_host, "", "elasticache host");
DEFINE_string(proxy_ip, "", "proxy server ip");
DEFINE_uint32(pair_sparse_boundary, 100000, "pair sparse boundary");
DEFINE_uint32(sparse_dense_boundary, 1000000, "sparse dense boundary");

std::vector<std::string> parse_proxy_server_list() {
    std::stringstream ss(FLAGS_proxy_server_list);
    std::vector<std::string> result;
    std::string token;
    while (std::getline(ss, token, ',')) {
        result.push_back(token);
    }
    return result;
}

struct exec_config {

    uint32_t request_id, partition_id;
    std::string graph_dir, result_dir, s3_bucket;
    bool no_pipeline, sparse_only, dense_only, dynamic_invoke, need_global_degree;
    int cores;
    CAAS_SAVE_MODE save_mode;
    json request;
    std::string reinvoke_command;
    int pr_iterations;
    uint32_t bfs_root, sssp_root;
    std::string elasticache_host, proxy_ip;
    bool elastic_proxy;
    int kill_wait_ms;
    uint32_t pair_sparse_boundary, sparse_dense_boundary;
    RUN_TYPE run_type;

    exec_config() {}
    
    exec_config(
        uint32_t request_id, uint32_t partition_id, 
        std::string graph_dir, std::string result_dir, std::string s3_bucket, 
        bool no_pipeline, bool sparse_only, bool dense_only, bool dynamic_invoke, bool need_global_degree,
        int cores, CAAS_SAVE_MODE save_mode,
        std::string elasticache_host, std::string proxy_ip, 
        bool elastic_proxy, int kill_wait_ms,
        RUN_TYPE run_type
    ):request_id(request_id), partition_id(partition_id),
      graph_dir(graph_dir), result_dir(result_dir), s3_bucket(s3_bucket), 
      no_pipeline(no_pipeline), sparse_only(sparse_only), dense_only(dense_only), dynamic_invoke(dynamic_invoke), 
      need_global_degree(need_global_degree),
      cores(cores), save_mode(save_mode), elasticache_host(elasticache_host), proxy_ip(proxy_ip), 
      elastic_proxy(elastic_proxy), kill_wait_ms(kill_wait_ms), 
      run_type(run_type) {
        if (no_pipeline) {
            VLOG(1) << "pipeline disabled";
        }
        if (dynamic_invoke) {
            VLOG(1) << "dynamic invoke enabled";
        }
        if (elastic_proxy) {
            VLOG(1) << "elastic proxy enabled";
        }
    }

    static exec_config *build_by_flags(){
        exec_config *config = new exec_config(
            FLAGS_request_id, FLAGS_partition_id,
            FLAGS_graph_dir, FLAGS_result_dir, FLAGS_s3_bucket, 
            FLAGS_no_pipeline, FLAGS_sparse_only, FLAGS_dense_only, FLAGS_dynamic_invoke, FLAGS_need_global_degree,
            FLAGS_cores, (CAAS_SAVE_MODE)FLAGS_save_mode, FLAGS_elasticache_host, FLAGS_proxy_ip, 
            FLAGS_elastic_proxy, FLAGS_kill_wait_ms,
            RUN_TYPE::LOCAL
        );
        config -> pr_iterations = FLAGS_pr_iterations;
        config -> bfs_root = FLAGS_bfs_root;
        config -> sssp_root = FLAGS_sssp_root;
        config -> pair_sparse_boundary = FLAGS_pair_sparse_boundary;
        config -> sparse_dense_boundary = FLAGS_sparse_dense_boundary;
        return config;
    }

    static exec_config *build_by_json(json request){
        std::string s3_bucket = request.value("s3_bucket", "");
        std::string elasticache_host = request.value("elasticache_host", "");
        std::string proxy_ip = request.value("proxy_ip", "");
        int kill_wait_ms = request.value("kill_wait_ms", 100000);
        exec_config *config = new exec_config(
            request["request_id"], request["partition_id"],
            request["graph_dir"], request["result_dir"], s3_bucket, 
            request["no_pipeline"], request["sparse_only"], request["dense_only"], request["dynamic_invoke"], request["need_global_degree"],
            request["cores"], (CAAS_SAVE_MODE)request["save_mode"], elasticache_host, proxy_ip, 
            request["elastic_proxy"], kill_wait_ms,
            RUN_TYPE::LAMBDA
        );
        config -> request = request;
        if (config -> get_app() == "bfs"){
            config -> bfs_root = request["bfs_root"];
        } else if (config -> get_app() == "pr") {
            config -> pr_iterations = request["pr_iterations"];
        } else if (config -> get_app() == "sssp"){
            config -> sssp_root = request["sssp_root"];
        }
        uint32_t pair_sparse_boundary = request.value("pair_sparse_boundary", 100000);
        uint32_t sparse_dense_boundary = request.value("sparse_dense_boundary", 1000000);
        config -> pair_sparse_boundary = pair_sparse_boundary;
        config -> sparse_dense_boundary = sparse_dense_boundary;
        return config;
    }

    bool enable_sdk() {
        return enable_s3_sdk() || enable_lambda_sdk() || enable_fargate_sdk();
    }

    bool enable_s3_sdk() {
        return save_mode == CAAS_SAVE_MODE::SAVE_S3;
    }

    bool enable_lambda_sdk() {
        return dynamic_invoke;
    }

    bool enable_fargate_sdk() {
        return elastic_proxy;
    }

    bool enable_elasticache_sdk() {
        return elastic_proxy;
    }
    
    // only support bfs,pr,sssp,cc
    std::string get_app() {
        if (FLAGS_bfs_root != 0xffffffff || request.contains("bfs_root")) {
            return "bfs";
        } else if (FLAGS_pr_iterations != -1 || request.contains("pr_iterations")) {
            return "pr";
        } else if (FLAGS_sssp_root != 0xffffffff || request.contains("sssp_root")){
            return "sssp";
        } else {
            return "cc";
        }
    }

    std::string build_reinvoke_command(int round, std::string proxy_server_host) {
        std::string app = get_app();
        reinvoke_command = "";
        reinvoke_command.reserve(256);
        if (run_type == RUN_TYPE::LOCAL) {
            reinvoke_command.append("sudo ./local_");
            reinvoke_command.append(app);
            reinvoke_command.append(" --request-id ");
            reinvoke_command.append(std::to_string(request_id));
            reinvoke_command.append(" --partition-id ");
            reinvoke_command.append(std::to_string(partition_id));
            reinvoke_command.append(" --graph-dir ");
            reinvoke_command.append(graph_dir);
            reinvoke_command.append(" --result-dir ");
            reinvoke_command.append(result_dir);
            reinvoke_command.append(" --cores ");
            reinvoke_command.append(std::to_string(cores));
            if (FLAGS_no_pipeline) {
                reinvoke_command.append(" --no-pipeline ");
            }
            if (FLAGS_sparse_only) {
                reinvoke_command.append(" --sparse-only ");
            }
            if (FLAGS_dense_only) {
                reinvoke_command.append(" --dense-only ");
            }
            if (FLAGS_need_global_degree) {
                reinvoke_command.append(" --need-global-degree ");
            }
            if (FLAGS_elastic_proxy) {
                reinvoke_command.append(" --elastic-proxy ");
                reinvoke_command.append(" --elasticache-host ");
                reinvoke_command.append(elasticache_host);
            } else {
                reinvoke_command.append(" --proxy-ip ");
                reinvoke_command.append(proxy_ip);
            }
            reinvoke_command.append(" --save-mode ");
            reinvoke_command.append(std::to_string((int)save_mode));
            if (enable_s3_sdk()) {
                reinvoke_command.append(" --s3-bucket ");
                reinvoke_command.append(s3_bucket);
            }
            if (FLAGS_dynamic_invoke) {
                reinvoke_command.append(" --dynamic-invoke ");
            }
            if (app == "bfs") {
                reinvoke_command.append(" --bfs-root ");
                reinvoke_command.append(std::to_string(FLAGS_bfs_root));
            }
            else if (app == "pr") {
                reinvoke_command.append(" --pr-iterations ");
                reinvoke_command.append(std::to_string(pr_iterations - round + 1));
            }
            else if (app == "sssp") {
                reinvoke_command.append(" --sssp-root ");
                reinvoke_command.append(std::to_string(FLAGS_sssp_root));
            }
            reinvoke_command.append(" --v 1 > ");
            reinvoke_command.append(std::to_string(partition_id));
            reinvoke_command.append(std::to_string(partition_id));
            reinvoke_command.append(".txt 2>&1");
        } else if (run_type == RUN_TYPE::LAMBDA) {
            reinvoke_command = "aws ";
            reinvoke_command.append(request["function_name"]);
            reinvoke_command.append(" ");
            request["dynamic_invoke"] = true;
            request["elastic_proxy"] = false;
            request["proxy_ip"] = proxy_server_host;
            if (app == "pr") {
                request["pr_iterations"] = pr_iterations - round + 1;
            } else if (app == "bfs"){
                request["bfs_root"] = bfs_root;
            } else if (app == "sssp"){
                request["sssp_root"] = sssp_root;
            }
            reinvoke_command.append(request.dump());
        } else {
            LOG(FATAL) << "run type not implemented";
        }
        VLOG(1) << "generated reinvoke command: " << reinvoke_command;
        return reinvoke_command;
    }

};

#endif