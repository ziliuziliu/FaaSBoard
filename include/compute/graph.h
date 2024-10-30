#ifndef _GRAPH_H
#define _GRAPH_H

#include "communication/caas.h"
#include "util/io.h"
#include "util/bitmap.h"
#include "util/types.h"
#include "util/json.h"

#include <algorithm>
#include <cstdint>

using json = nlohmann::json;

struct graph_meta {

    uint32_t total_v, total_e;
    
    graph_meta() {}

    graph_meta(uint32_t total_v, uint32_t total_e): total_v(total_v), total_e(total_e) {}

};

template <class vwT, class ewT>
class graph {

public:

    int index;
    graph_meta g_meta;
    bool weighted, manage;
    uint32_t from_source, to_source, from_dest, to_dest, edges;
    uint32_t *in_offset, *in_source, *out_offset, *out_dest;
    ewT *in_weight, *out_weight;
    comm_object<vwT> *in_segment, *out_segment;
    comm_object<uint32_t> *vote_object;
    uint8_t reduce_op;
    vwT base_vertex_value;

    graph() {}

    graph(
        int index, graph_meta g_meta, uint32_t from_source, uint32_t to_source, uint32_t from_dest, uint32_t to_dest, 
        uint32_t edges, uint8_t reduce_op, vwT base_vertex_value
    ) : g_meta(g_meta), from_source(from_source), to_source(to_source), from_dest(from_dest), to_dest(to_dest), 
        edges(edges), reduce_op(reduce_op), base_vertex_value(base_vertex_value) {
        weighted = !std::is_same<ewT, void *>::value;
        manage = std::max(from_source, from_dest) > std::min(to_source, to_dest);
        out_offset = new uint32_t[to_source - from_source + 1]();
        out_dest = new uint32_t[edges]();
        in_offset = new uint32_t[to_dest - from_dest + 1]();
        in_source = new uint32_t[edges]();
        if (weighted) {
            in_weight = new ewT[edges]();
            out_weight = new ewT[edges]();
        }
        VLOG(1) << "graph " << index << " [ " 
            << from_source << ", " << to_source - 1 << " ] -> [ "
            << from_dest << ", " << to_dest - 1 << " ]";
    }

    bool check_diagonal() {
        return in_segment -> start_index == out_segment -> start_index 
            && in_segment -> vec_len == out_segment -> vec_len;
    }

    void read_csr(std::string path) {
        read_csr_util(path, in_offset, in_source, out_offset, out_dest, in_weight, out_weight, 
            weighted, to_source - from_source, to_dest - from_dest, edges);
    }

    void set_comm_object(comm_object<vwT> *in_segment, comm_object<vwT> *out_segment, comm_object<uint32_t> *vote_object) {
        this -> in_segment = in_segment;
        this -> out_segment = out_segment;
        this -> vote_object = vote_object;
    }

    void connect(uint32_t request_id) {
        in_segment -> caas_connect(request_id);
        out_segment -> caas_connect(request_id);
        vote_object -> caas_connect(request_id);
    }

    void disconnect() {
        in_segment -> caas_disconnect();
        out_segment -> caas_disconnect();
        vote_object -> caas_disconnect();
    }

    void begin(int round, int index, auto vertex_initialize_func) {
        uint32_t activated = 0;
        uint32_t start_source = in_segment -> start_index;
        uint32_t end_source = in_segment -> start_index + in_segment -> vec_len;
        #pragma omp parallel for reduction(+:activated)
        for (uint32_t v = start_source; v < end_source; v++) {
            activated += vertex_initialize_func(in_segment, v);
        }
        vote_object -> vec[0] = activated;
    }

    void exec_each(int round, int index, vwT vertex_initial_value, auto sparse_func, auto dense_func) {
        uint32_t active_edges = 0;
        in_segment -> print(round);
        uint32_t start_source = in_segment -> start_index;
        uint32_t end_source = in_segment -> start_index + in_segment -> vec_len;
        #pragma omp parallel for reduction(+:active_edges)
        for (uint32_t v = start_source; v < end_source; v++) {
            if (in_segment -> bm -> exist(v - start_source)) {
                active_edges += out_offset[v + 1 - from_source] - out_offset[v - from_source];
            }
        }
        bool sparse = active_edges < edges / 20;
        if (sparse) {
            VLOG(1) << "running in sparse mode";
            uint32_t start_source = in_segment -> start_index;
            uint32_t end_source = in_segment -> start_index + in_segment -> vec_len;
            #pragma omp parallel for schedule(dynamic, 1000)
            for (uint32_t v = start_source; v < end_source; v++) {
                if (in_segment -> bm -> exist(v - start_source)) {
                    uint32_t *out_dst = out_dest + out_offset[v - from_source];
                    ewT *out_w = weighted ? out_weight + out_offset[v - from_source] : nullptr;
                    uint32_t out_d = out_offset[v + 1 - from_source] - out_offset[v - from_source];
                    sparse_func(in_segment, out_segment, v, out_dst, out_w, out_d);
                }
            }
        } else {
            timer t;
            VLOG(1) << "running in dense mode";
            uint32_t start_dest = out_segment -> start_index;
            uint32_t end_dest = out_segment -> start_index + out_segment -> vec_len;
            t.tick("dense mode");
            #pragma omp parallel for schedule(dynamic, 1000)
            for (uint32_t v = start_dest; v < end_dest; v++) {
                uint32_t *in_src = in_source + in_offset[v - from_dest];
                ewT *in_w = weighted ? in_weight + in_offset[v - from_dest] : nullptr;
                uint32_t in_d = in_offset[v + 1 - from_dest] - in_offset[v - from_dest];
                dense_func(in_segment, out_segment, v, in_src, in_w, in_d);
            }
            t.from_tick();
        }
        uint32_t activated = 0;
        activated += out_segment -> bm -> get_size();
        out_segment -> print(round);
        vote_object -> vec[0] = activated;
    }

    void exec_diagonal(int round, int index, auto reduce_func) {
        if (check_diagonal()) {
            out_segment -> print(round);
            in_segment -> bm -> clear();
            uint32_t start = in_segment -> start_index;
            uint32_t end = in_segment -> start_index + in_segment -> vec_len;
            #pragma omp parallel for schedule(dynamic, 1000)
            for (uint32_t v = start; v < end; v++) {
                if (out_segment -> bm -> exist(v - start)) {
                    reduce_func(in_segment, out_segment, v);
                }
            }
            in_segment -> print(round);
        }
    }

    uint32_t vote() {
        vote_object -> caas_do();
        return vote_object -> vec[0];
    }

    void save_result(std::string path) {
        std::ofstream file(path);
        if (!file.is_open()) {
            LOG(FATAL) << "could not open the file " << path;
        }
        if (check_diagonal()) {
            uint32_t start = in_segment -> start_index;
            uint32_t end = in_segment -> start_index + in_segment -> vec_len;
            save_result_util<vwT>(file, start, end, in_segment -> vec);
        }
    }

};

#endif
