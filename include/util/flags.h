#ifndef _FLAGS_H
#define _FLAGS_H

#include <gflags/gflags.h>
#include <vector>
#include <iostream>
#include <sstream>

DEFINE_uint32(request_id, 0, "request id");
DEFINE_uint32(bfs_root, 0, "root vertex for bfs");
DEFINE_string(proxy_server_list, "", "list of available proxy server addresses (ip) separated by comma");
DEFINE_string(graph_dir, "", "directory for graph dataset");

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