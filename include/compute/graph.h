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
    std::vector<comm_object<vwT> *> in_segments, out_segments;
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

    void read_csr(std::string path) {
        read_csr_util(path, in_offset, in_source, out_offset, out_dest, in_weight, out_weight, 
            weighted, to_source - from_source, to_dest - from_dest, edges);
    }

    void set_segment_connection(json meta) {
        uint8_t comm_type = meta["comm_type"];
        std::string meta_server_addr = meta["meta_server_addr"];
        int meta_server_port = meta["meta_server_port"];
        json recv = meta["recv"];
        for (int i = 0; i < (int)recv.size(); i++) {
            json item = recv[i];
            uint32_t start = item["start"], end = item["end"];
            uint8_t data_type = caas_get_data_type<vwT>();
            in_segments.push_back(caas_make_comm_object<vwT>(
                comm_type, meta_server_addr, meta_server_port, 
                item["object_id"], end - start, true, start, 
                item["root"], item["members"], data_type, CAAS_MASKED_BROADCAST, reduce_op,
                base_vertex_value
            ));
        }
        json send = meta["send"];
        for (int i = 0; i < (int)send.size(); i++) {
            json item = send[i];
            uint32_t start = item["start"], end = item["end"];
            uint8_t data_type = caas_get_data_type<vwT>();
            out_segments.push_back(caas_make_comm_object<vwT>(
                comm_type, meta_server_addr, meta_server_port,
                item["object_id"], end - start, true, start, 
                item["root"], item["members"], data_type, CAAS_MASKED_REDUCE, reduce_op,
                base_vertex_value
            ));
        }
        json vote_meta = meta["vote"];
        vote_object = caas_make_comm_object<uint32_t>(
            comm_type, meta_server_addr, meta_server_port,
            vote_meta["object_id"], 1, false, 0, 
            false, vote_meta["members"], CAAS_UINT32, CAAS_ALLREDUCE, CAAS_ADD,
            0
        );
    }

    void connect(uint32_t request_id) {
        for (auto seg : in_segments) {
            seg -> caas_connect(request_id);
        }
        for (auto seg : out_segments) {
            seg -> caas_connect(request_id);
        }
        vote_object -> caas_connect(request_id);
    }

    void disconnect() {
        for (auto seg : in_segments) {
            seg -> caas_disconnect();
        }
        for (auto seg : out_segments) {
            seg -> caas_disconnect();
        }
        vote_object -> caas_disconnect();
    }

    void in(int round, int index) {
        for (auto in_seg : in_segments) {
            in_seg -> print(round, index);
            in_seg -> caas_do();
            in_seg -> print(round, index);
        }
    }

    void out(int round, int index) {
        for (auto out_seg : out_segments) {
            out_seg -> print(round, index);
            out_seg -> caas_do();
            out_seg -> print(round, index);
        }
    }

    void begin(int round, int index, auto vertex_initialize_func) {
        uint32_t activated = 0;
        for (auto in_seg : in_segments) {
            uint32_t start_source = in_seg -> start_index;
            uint32_t end_source = in_seg -> start_index + in_seg -> vec_len;
            #pragma omp parallel for reduction(+:activated)
            for (uint32_t v = start_source; v < end_source; v++) {
                activated += vertex_initialize_func(in_seg, v);
            }
        }
        vote_object -> vec[0] = activated;
    }

    void exec_each(int round, int index, vwT vertex_initial_value, auto sparse_func, auto dense_func) {
        for (auto out_seg : out_segments) {
            out_seg -> bm -> clear();
            #pragma omp parallel for
            for (uint32_t i = 0; i < out_seg -> vec_len; i++)
                out_seg -> vec[i] = vertex_initial_value;
        }
        uint32_t active_edges = 0;
        for (auto in_seg : in_segments) {
            in_seg -> print(round, index);
            uint32_t start_source = in_seg -> start_index;
            uint32_t end_source = in_seg -> start_index + in_seg -> vec_len;
            #pragma omp parallel for reduction(+:active_edges)
            for (uint32_t v = start_source; v < end_source; v++) {
                if (in_seg -> bm -> exist(v - start_source)) {
                    active_edges += out_offset[v + 1 - from_source] - out_offset[v - from_source];
                }
            }
        }
        bool sparse = active_edges < edges / 20;
        if (sparse) {
            for (auto in_seg : in_segments) {
                uint32_t start_source = in_seg -> start_index;
                uint32_t end_source = in_seg -> start_index + in_seg -> vec_len;
                #pragma omp parallel for schedule(dynamic, 1000)
                for (uint32_t v = start_source; v < end_source; v++) {
                    if (in_seg -> bm -> exist(v - start_source)) {
                        for (uint32_t off = out_offset[v - from_source]; off < out_offset[v + 1 - from_source]; off++) {
                            uint32_t dest = out_dest[off];
                            ewT weight = (weighted) ? out_weight[off] : ewT();
                            auto out_seg = comm_object<vwT>::search(dest, out_segments);
                            sparse_func(in_seg, out_seg, v, dest, weight);
                        }
                    }
                }
            }
        } else {
            for (auto out_seg : out_segments) {
                uint32_t start_dest = out_seg -> start_index;
                uint32_t end_dest = out_seg -> start_index + out_seg -> vec_len;
                #pragma omp parallel for schedule(dynamic, 1000)
                for (uint32_t v = start_dest; v < end_dest; v++) {
                    for (uint32_t off = in_offset[v - from_dest]; off < in_offset[v + 1 - from_dest]; off++) {
                        uint32_t source = in_source[off];
                        ewT weight = (weighted) ? in_weight[off] : ewT();
                        auto in_seg = comm_object<vwT>::search(source, in_segments);
                        if (in_seg -> bm -> exist(source - in_seg -> start_index)) {
                            dense_func(in_seg, out_seg, source, v, weight);
                        }
                    }
                }
            }
        }
        uint32_t activated = 0;
        for (auto out_seg : out_segments) {
            activated += out_seg -> bm -> get_size();
            out_seg -> print(round, index);
        }
        vote_object -> vec[0] = activated;
    }

    void exec_diagonal(int round, int index, auto reduce_func) {
        for (auto in_seg : in_segments)
            for (auto out_seg : out_segments) {
                if (in_seg -> start_index == out_seg -> start_index
                    && in_seg -> vec_len == out_seg -> vec_len) {
                        out_seg -> print(round, index);
                        in_seg -> bm -> clear();
                        uint32_t start = in_seg -> start_index;
                        uint32_t end = in_seg -> start_index + in_seg -> vec_len;
                        #pragma omp parallel for schedule(dynamic, 1000)
                        for (uint32_t v = start; v < end; v++) {
                            if (out_seg -> bm -> exist(v - start)) {
                                reduce_func(in_seg, out_seg, v);
                            }
                        }
                        in_seg -> print(round, index);
                    }
            }
    }

    uint32_t vote() {
        vote_object -> caas_do();
        return vote_object -> vec[0];
    }

    void save_result(std::string path) {
        std::ofstream file(path);
        for (auto in_seg : in_segments)
            for (auto out_seg : out_segments) {
                if (in_seg -> start_index == out_seg -> start_index
                    && in_seg -> vec_len == out_seg -> vec_len) {
                        uint32_t start = in_seg -> start_index;
                        uint32_t end = in_seg -> start_index + in_seg -> vec_len;
                        save_result_util<vwT>(file, start, end, in_seg -> vec);
                    }
            }
    }

};

#endif
