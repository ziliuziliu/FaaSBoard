#ifndef _FLAGS_H
#define _FLAGS_H

#include "util/types.h"
#include "util/log.h"

#include <gflags/gflags.h>
#include <vector>
#include <iostream>
#include <sstream>
#include "util/types.h"

DEFINE_string(graph_root_dir, ".", "root directory for graph dataset in csr binary");
DEFINE_string(graph_file, "", "original graph dataset file");
DEFINE_uint32(vertices, 0, "#vertices");
DEFINE_uint32(edges, 0, "#edges");
DEFINE_uint32(request_id, 0, "request id");
DEFINE_uint32(bfs_root, 0, "root vertex for bfs");
DEFINE_uint32(sssp_root, 0, "root vertex for sssp");
DEFINE_int32(pr_iterations, 0, "iterations for pagerank");
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
DEFINE_string(ewT, "empty", "Whether to specify the edge weight type (empty,int,uint32_t,float)");
DEFINE_string(strategy, "checkerboard", "partitioning strategy: row, column, mondriaan_row_column, mondriaan_column, cycle, stagger, checkerboard");

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

    std::string graph_dir, result_dir, meta_server_addr, s3_bucket;
    bool no_pipeline, sparse_only, dense_only;
    int cores;
    CAAS_SAVE_MODE save_mode;

    exec_config() {}
    
    exec_config(
        std::string graph_dir, std::string result_dir, std::string meta_server_addr, std::string s3_bucket, 
        bool no_pipeline, bool sparse_only, bool dense_only, int cores, CAAS_SAVE_MODE save_mode
    ):graph_dir(graph_dir), result_dir(result_dir), meta_server_addr(meta_server_addr), s3_bucket(s3_bucket), 
      no_pipeline(no_pipeline), sparse_only(sparse_only), dense_only(dense_only), cores(cores), save_mode(save_mode) {
        check();
    }

    void check() {
        if (enable_s3() && s3_bucket == "") {
            LOG(FATAL) << "need to provide s3 bucket name";
        }
    }

    bool enable_s3() {
        return save_mode == CAAS_SAVE_MODE::SAVE_S3;
    }

};

#endif