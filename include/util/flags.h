#ifndef _FLAGS_H
#define _FLAGS_H

#include <gflags/gflags.h>
#include <vector>
#include <iostream>
#include <sstream>

DEFINE_string(graph_root_dir, "", "root directory for graph dataset in csr binary");
DEFINE_string(graph_file, "", "original graph dataset file");
DEFINE_uint32(vertices, 0, "#vertices");
DEFINE_uint32(edges, 0, "#edges");
DEFINE_uint32(request_id, 0, "request id");
DEFINE_uint32(bfs_root, 0, "root vertex for bfs");
DEFINE_string(graph_dir, "", "directory for graph dataset in csr binary");
DEFINE_string(proxy_server_list, "", "list of available proxy server addresses (ip) separated by comma");
DEFINE_uint32(cores, 0, "cores to use");
DEFINE_string(application, "", "application type (bfs, cc, pr, sssp)");
DEFINE_bool(no_pipeline, false, "no in-exec_each-out-exec_diagonal pipeline");
DEFINE_int32(partitions, 0, "how many functions to hold the graph");
DEFINE_bool(sparse_only, false, "sparse mode");
DEFINE_bool(dense_only, false, "dense mode");

std::vector<std::string> parse_proxy_server_list() {
    std::stringstream ss(FLAGS_proxy_server_list);
    std::vector<std::string> result;
    std::string token;
    while (std::getline(ss, token, ',')) {
        result.push_back(token);
    }
    return result;
}

#endif