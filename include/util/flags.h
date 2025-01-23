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
DEFINE_uint32(edges, 0, "#edges");
DEFINE_uint32(request_id, 0, "request id");
DEFINE_uint32(bfs_root, 0xffffffff, "root vertex for bfs");
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
DEFINE_int32(save_mode, 1, "result save mode (0: no save, 1: local disk, 2: s3)");
DEFINE_string(s3_bucket, "", "s3 bucket name");
DEFINE_bool(undirected, false, "whether it's undirected graph (0: directed, 1: undirected)");
DEFINE_bool(dynamic_invoke, false, "enable dynamic invoke");
DEFINE_uint32(partition_id, 0xffffffff, "partition id (0-based)");
DEFINE_int32(kill_wait_ms, 100000, "wait time to kill process");

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
    std::string graph_dir, result_dir, meta_server_addr, s3_bucket;
    bool no_pipeline, sparse_only, dense_only, dynamic_invoke;
    int cores;
    CAAS_SAVE_MODE save_mode;
    RUN_TYPE run_type;
    json request;
    std::string reinvoke_command;

    exec_config() {}
    
    exec_config(
        uint32_t request_id, uint32_t partition_id, 
        std::string graph_dir, std::string result_dir, std::string meta_server_addr, std::string s3_bucket, 
        bool no_pipeline, bool sparse_only, bool dense_only, bool dynamic_invoke, int cores, CAAS_SAVE_MODE save_mode,
        RUN_TYPE run_type
    ):request_id(request_id), partition_id(partition_id),
      graph_dir(graph_dir), result_dir(result_dir), meta_server_addr(meta_server_addr), s3_bucket(s3_bucket), 
      no_pipeline(no_pipeline), sparse_only(sparse_only), dense_only(dense_only), dynamic_invoke(dynamic_invoke), 
      cores(cores), save_mode(save_mode), run_type(run_type) {
        if (no_pipeline) {
            VLOG(1) << "pipeline disabled";
        }
        if (dynamic_invoke) {
            VLOG(1) << "dynamic invoke enabled";
        }
        check();
    }

    static exec_config *build_by_flags(){
        exec_config *config = new exec_config(
            FLAGS_request_id, FLAGS_partition_id,
            FLAGS_graph_dir, FLAGS_result_dir, FLAGS_meta_server, FLAGS_s3_bucket, 
            FLAGS_no_pipeline, FLAGS_sparse_only, FLAGS_dense_only, FLAGS_dynamic_invoke, 
            FLAGS_cores, (CAAS_SAVE_MODE)FLAGS_save_mode, RUN_TYPE::LOCAL
        );
        return config;
    }

    static exec_config *build_by_json(json request){
        exec_config *config = new exec_config(
            request["request_id"], request["partition_id"],
            request["graph_dir"], request["result_dir"], request["meta_server"], request["s3_bucket"], 
            request["no_pipeline"], request["sparse_only"], request["dense_only"], request["dynamic_invoke"], 
            request["cores"], (CAAS_SAVE_MODE)request["save_mode"], RUN_TYPE::LAMBDA
        );
        config -> request = request;
        return config;
    }

    void check() {
        if (partition_id == 0xffffffff) {
            LOG(FATAL) << "need to provide partition id";
        }
        if (enable_s3() && s3_bucket == "") {
            LOG(FATAL) << "need to provide s3 bucket name";
        }
    }

    bool enable_s3() {
        return save_mode == CAAS_SAVE_MODE::SAVE_S3;
    }
    
    std::string get_app() {
        if (FLAGS_bfs_root != 0xffffffff || request.contains("bfs_root")) {
            return "bfs";
        } else if (FLAGS_pr_iterations != -1 || request.contains("pr_iterations")) {
            return "pr";
        } else {
            LOG(FATAL) << "app not implemented";
        }
    }

    std::string build_reinvoke_command(int round) {
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
            reinvoke_command.append(" --meta-server ");
            reinvoke_command.append(meta_server_addr);
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
            reinvoke_command.append(" --save-mode ");
            reinvoke_command.append(std::to_string((int)save_mode));
            if (enable_s3()) {
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
                reinvoke_command.append(std::to_string(FLAGS_pr_iterations - round + 1));
            }
            reinvoke_command.append(" --v 1 > ");
            reinvoke_command.append(std::to_string(partition_id));
            reinvoke_command.append(std::to_string(partition_id));
            reinvoke_command.append(".txt 2>&1");
        } else if (run_type == RUN_TYPE::LAMBDA) {
            reinvoke_command = "aws lambda invoke --function-name ";
            reinvoke_command.append(app);
            reinvoke_command.append("_");
            reinvoke_command.append(std::to_string(partition_id));
            reinvoke_command.append(" --payload '");
            if (app == "pr") {
                request["pr_iterations"] = (int)request["pr_iterations"] - round + 1;
            }
            reinvoke_command.append(request.dump());
            reinvoke_command.append("'");
            reinvoke_command.append(" result");
            reinvoke_command.append(std::to_string(partition_id));
            reinvoke_command.append(std::to_string(partition_id));
            reinvoke_command.append(".txt");
        } else {
            LOG(FATAL) << "run type not implemented";
        }
        VLOG(1) << "generated reinvoke command: " << reinvoke_command;
        return reinvoke_command;
    }

};

#endif