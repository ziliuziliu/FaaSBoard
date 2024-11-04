#ifndef _GRAPH_H
#define _GRAPH_H

#include "communication/caas.h"
#include "util/io.h"
#include "util/bitmap.h"
#include "util/types.h"
#include "util/json.h"
#include "util/flags.h"

#include <algorithm>
#include <cstdint>
#include <atomic>
#include <functional>

using json = nlohmann::json;

struct graph_meta {

    uint32_t total_v, total_e;
    
    graph_meta() {}

    graph_meta(uint32_t total_v, uint32_t total_e): total_v(total_v), total_e(total_e) {}

};

struct thread_state {

    std::atomic<uint32_t> curr;

    uint32_t end, state;

    thread_state() {}

    thread_state(uint32_t curr, uint32_t end) {
        std::atomic_init(&this -> curr, curr);
        this -> end = end;
        this -> state = WORKING;
    }

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
    std::function<int(comm_object<vwT> *, uint32_t)> begin_func;
    std::function<
        void(comm_object<vwT> *, comm_object<vwT> *, uint32_t, uint32_t *, ewT *, uint32_t)
    > sparse_func, dense_func;
    std::function<void(comm_object<uint32_t> *, comm_object<uint32_t> *, uint32_t)> reduce_func;

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

    void begin(int round, int index) {
        uint32_t activated = 0;
        uint32_t start_source = in_segment -> start_index;
        uint32_t end_source = in_segment -> start_index + in_segment -> vec_len;
        #pragma omp parallel for reduction(+:activated)
        for (uint32_t v = start_source; v < end_source; v++) {
            activated += begin_func(in_segment, v);
        }
        vote_object -> vec[0] = activated;
    }

    void exec_sparse(uint32_t begin, uint32_t end) {
        uint32_t start_source = in_segment -> start_index;
        for (uint32_t v = begin; v < end; v++) {
            if (in_segment -> bm -> exist(v - start_source)) {
                uint32_t *out_dst = out_dest + out_offset[v - from_source];
                ewT *out_w = weighted ? out_weight + out_offset[v - from_source] : nullptr;
                uint32_t out_d = out_offset[v + 1 - from_source] - out_offset[v - from_source];
                sparse_func(in_segment, out_segment, v, out_dst, out_w, out_d);
            }
        }
    }

    void exec_dense(uint32_t begin, uint32_t end) {
        for (uint32_t v = begin; v < end; v++) {
            uint32_t *in_src = in_source + in_offset[v - from_dest];
            ewT *in_w = weighted ? in_weight + in_offset[v - from_dest] : nullptr;
            uint32_t in_d = in_offset[v + 1 - from_dest] - in_offset[v - from_dest];
            dense_func(in_segment, out_segment, v, in_src, in_w, in_d);
        }
    }

    void work_stealing(uint32_t start_v, uint32_t end_v, auto selected) {
        uint32_t interval = end_v - start_v;
        std::vector<thread_state *> thread_states;
        for (int i = 0; i < (int)FLAGS_cores; i++) {
            uint32_t curr = start_v + interval / FLAGS_cores * i;
            uint32_t end = start_v + interval / FLAGS_cores * (i + 1);
            if (i == (int)FLAGS_cores - 1) {
                end = end_v;
            }
            thread_states.push_back(new thread_state(curr, end));
        }
        auto exec_func = [&](int index){
            while (true) {
                uint32_t begin = thread_states[index] -> curr.fetch_add(64);
                if (begin >= thread_states[index] -> end) {
                    break;
                }
                uint32_t end = begin + 64 > thread_states[index] -> end ? thread_states[index] -> end : begin + 64;
                selected(begin, end);
            }
            thread_states[index] -> state = STEALING;
            for (int offset = 1; offset < (int)FLAGS_cores; offset++) {
                int new_index = (index + offset) % FLAGS_cores;
                if (thread_states[new_index] -> state == STEALING) {
                    continue;
                }
                while (true) {
                    uint32_t begin = thread_states[new_index] -> curr.fetch_add(64);
                    if (begin >= thread_states[new_index] -> end) {
                        break;
                    }
                    uint32_t end = begin + 64 > thread_states[new_index] -> end ? thread_states[new_index] -> end : begin + 64;
                    selected(begin, end);
                }
            }
        };
        std::vector<std::thread> work_threads;
        for (int i = 0; i < (int)FLAGS_cores; i++) {
            work_threads.push_back(std::thread(exec_func, i));
        }
        for (int i = 0; i < (int)FLAGS_cores; i++) {
            work_threads[i].join();
        }
    }

    void exec_each(int round, int index, vwT vertex_initial_value) {
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
            timer t;
            t.tick("sparse mode");
            work_stealing(start_source, end_source, [this](uint32_t start, uint32_t end){
                exec_sparse(start, end);
            });
            t.from_tick();
        } else {
            VLOG(1) << "running in dense mode";
            uint32_t start_dest = out_segment -> start_index;
            uint32_t end_dest = out_segment -> start_index + out_segment -> vec_len;
            timer t;
            t.tick("dense mode");
            work_stealing(start_dest, end_dest, [this](uint32_t start, uint32_t end){
                exec_dense(start, end);
            });
            t.from_tick();
        }
        uint32_t activated = 0;
        activated += out_segment -> bm -> get_size();
        out_segment -> print(round);
        vote_object -> vec[0] = activated;
    }

    void exec_diagonal(int round, int index) {
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
