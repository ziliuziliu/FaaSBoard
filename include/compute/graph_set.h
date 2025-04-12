#ifndef _GRAPH_SET_H
#define _GRAPH_SET_H

#include "graph.h"
#include "util/json.h"
#include "util/flags.h"
#include "util/timer.h"
#include "util/mpmc/blockingconcurrentqueue.h"

#include <fstream>
#include <vector>
#include <omp.h>
#include <atomic>

using json = nlohmann::json;

template<class vwT, class ewT>
class graph_set {

public:

    graph_meta meta;
    std::vector<graph<vwT, ewT> *> graphs;
    std::unordered_map<uint32_t, comm_object<vwT> *> in_segments, out_segments;
    comm_object<uint32_t> *vote_object;
    vwT base_vertex_value;
    exec_config *config;
    bool stateful;
    s3_sdk *s_sdk;
    std::atomic<uint64_t> total_msg_size;

    graph_set(CAAS_REDUCE_OP reduce_op, vwT base_vertex_value, exec_config *config)
        :base_vertex_value(base_vertex_value), config(config) {
        in_segments = std::unordered_map<uint32_t, comm_object<vwT> *>();
        out_segments = std::unordered_map<uint32_t, comm_object<vwT> *>();
        vote_object = nullptr;
        stateful = false;
        total_msg_size.store(0);
        VLOG(1) << "aws sdk init";
        if (config -> enable_sdk()) {
            sdk_init();
        }
        if (config -> enable_s3_sdk()) {
            s_sdk = new s3_sdk();
        }
        std::ifstream meta_file(config -> graph_dir + "/graphs.meta");
        if (!meta_file.is_open()) {
            LOG(FATAL) << "could not open the file " << config -> graph_dir + "/graphs.meta " << strerror(errno);
        }
        std::stringstream meta_buffer;
        meta_buffer << meta_file.rdbuf();
        json metadata = json::parse(meta_buffer.str());
        meta = graph_meta(metadata["total_v"], metadata["total_e"]);
        omp_set_num_threads(metadata["graphs"].size());
        int graph_cnt = (int)metadata["graphs"].size();
        graphs.resize(graph_cnt);
        #pragma omp parallel for
        for (int i = 0; i < graph_cnt; i++) {
            json item = metadata["graphs"][i];
            graph<vwT, ewT> *newgraph = new graph<vwT, ewT>(
                i, meta, item["from_source"], item["to_source"], item["from_dest"], item["to_dest"], 
                item["edges"], reduce_op, base_vertex_value, config
            );
            newgraph -> read_csr(
                config -> graph_dir + "/graph" + std::to_string(i) + ".csr.in",
                config -> graph_dir + "/graph" + std::to_string(i) + ".csr.out"
            );
            #pragma omp critical 
            {
                auto objects = make_comm_object(item["comm"], reduce_op, base_vertex_value, config);
                comm_object<vwT> *in_segment = std::get<0>(objects);
                comm_object<vwT> *out_segment = std::get<1>(objects);
                comm_object<uint32_t> *vote_object = std::get<2>(objects);
                stateful |= in_segment -> root | out_segment -> root;
                newgraph -> set_comm_object(in_segment, out_segment, vote_object);
                graphs[i] = newgraph;
                in_segment -> related_graph_index.push_back(i);
                out_segment -> related_graph_index.push_back(i);
            }
        }
        std::vector<uint32_t> object_ids;
        VLOG(1) << "has object 0";
        for (auto it = in_segments.begin(); it != in_segments.end(); it++) {
            VLOG(1) << "has object " << it -> first;
        }
        for (auto it = out_segments.begin(); it != out_segments.end(); it++) {
            VLOG(1) << "has object " << it -> first;
        }
    }

    std::tuple<comm_object<vwT> *, comm_object<vwT> *, comm_object<uint32_t> *> make_comm_object(
        json meta, CAAS_REDUCE_OP reduce_op, vwT base_vertex_value, exec_config *config
    ) {
        comm_object<vwT> *in_segment = nullptr, *out_segment = nullptr;
        CAAS_COMM_MODE comm_type = meta["comm_type"];
        json recv = meta["recv"];
        CHECK(recv.size() == 1) << "have to be just 1 in segment";
        for (int i = 0; i < (int)recv.size(); i++) {
            json item = recv[i];
            uint32_t start = item["start"], end = item["end"];
            CAAS_TYPE data_type = caas_get_data_type<vwT>();
            uint32_t object_id = item["object_id"];
            if (!in_segments.count(object_id)) {
                in_segments[object_id] = caas_make_comm_object<vwT>(
                    comm_type, item["object_id"], end - start, true, start, 
                    item["root"], item["instances"], item["members"], data_type, CAAS_OP::MASKED_BROADCAST, reduce_op,
                    base_vertex_value, config
                );
            }
            in_segment = in_segments[object_id];
            in_segment -> colocated_member++;
            if (item["root"]) {
                in_segment -> update_root();
            }
        }
        json send = meta["send"];
        CHECK(send.size() == 1) << "have to be just 1 out segment";
        for (int i = 0; i < (int)send.size(); i++) {
            json item = send[i];
            uint32_t start = item["start"], end = item["end"];
            CAAS_TYPE data_type = caas_get_data_type<vwT>();
            uint32_t object_id = item["object_id"];
            if (!out_segments.count(object_id)) {
                out_segments[object_id] = caas_make_comm_object<vwT>(
                    comm_type, item["object_id"], end - start, true, start, 
                    item["root"], item["instances"], item["members"], data_type, CAAS_OP::MASKED_REDUCE, reduce_op,
                    base_vertex_value, config
                );
            }
            out_segment = out_segments[object_id];
            out_segment -> colocated_member++;
            if (item["root"]) {
                out_segment -> update_root();
            }
        }
        json vote_meta = meta["vote"];
        if (vote_object == nullptr) {
            vote_object = caas_make_comm_object<uint32_t>(
                comm_type, vote_meta["object_id"], 1, false, 0, 
                false, vote_meta["instances"], vote_meta["members"], CAAS_TYPE::UINT32, CAAS_OP::ALLREDUCE, CAAS_REDUCE_OP::ADD,
                0, config
            );
        }
        return std::make_tuple(in_segment, out_segment, vote_object);
    }

    void connect(uint32_t request_id, uint32_t partition_id) {
        VLOG(1) << "connect to proxy";
        for (auto it = in_segments.begin(); it != in_segments.end(); it++) {
            it -> second -> request_id = request_id;
            it -> second -> data[0] = request_id;
        }
        for (auto it = out_segments.begin(); it != out_segments.end(); it++) {
            it -> second -> request_id = request_id;
            it -> second -> data[0] = request_id;
        }

        vote_object -> caas_connect(request_id, partition_id);
    }

    void disconnect() {
        VLOG(1) << "disconnect with proxy";
        vote_object -> caas_disconnect();
    }

    void set_begin_func(auto begin_func) {
        for (int i = 0; i < (int)graphs.size(); i++) {
            graphs[i] -> begin_func = begin_func;
        }
    }

    void set_sparse_func(auto sparse_func) {
        for (int i = 0; i < (int)graphs.size(); i++) {
            graphs[i] -> sparse_func = sparse_func;
        }
    }

    void set_dense_func(auto dense_func) {
        for (int i = 0; i < (int)graphs.size(); i++) {
            graphs[i] -> dense_func = dense_func;
        }
    }

    void set_reduce_func(auto reduce_func) {
        for (int i = 0; i < (int)graphs.size(); i++) {
            graphs[i] -> reduce_func = reduce_func;
        }
    }

    void update_config(exec_config *config) {
        this -> config = config;
        for (int i = 0; i < (int)graphs.size(); i++) {
            graphs[i] -> config = config;
        }
        for (auto it = in_segments.begin(); it != in_segments.end(); it++) {
            it -> second -> config = config;
        }
        for (auto it = out_segments.begin(); it != out_segments.end(); it++) {
            it -> second -> config = config;
        }
        vote_object -> config = config;
        total_msg_size.store(0);
    }

    void begin(int round) {
        omp_set_num_threads(config -> cores);
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " vertex initialize";
            graphs[i] -> begin(round, i);
        }
    }

    void in(int round) {
        auto in_function = [&](comm_object<vwT> *in_segment){
            in_segment -> print(round);
            total_msg_size.fetch_add(in_segment -> caas_do(false, round, -1));
            in_segment -> print(round);
        };
        std::vector<std::thread> in_threads;
        for (auto it = in_segments.begin(); it != in_segments.end(); it++) {
            in_threads.push_back(std::thread(in_function, it -> second));
        }
        for (int i = 0; i < (int)in_threads.size(); i++) {
            in_threads[i].join();
        }
    }

    void out(int round) {
        auto out_function = [&](comm_object<vwT> *out_segment){
            out_segment -> print(round);
            total_msg_size.fetch_add(out_segment -> caas_do(false, round, -1));
            out_segment -> print(round);
        };
        std::vector<std::thread> out_threads;
        for (auto it = out_segments.begin(); it != out_segments.end(); it++) {
            out_threads.push_back(std::thread(out_function, it -> second));
        }
        for (int i = 0; i < (int)out_threads.size(); i++) {
            out_threads[i].join();
        }
    }

    void exec_each_initialize() {
        for (auto it = out_segments.begin(); it != out_segments.end(); it++) {
            comm_object<vwT> *out_segment = it -> second;
            out_segment -> bm -> clear();
            out_segment -> finish = 0;
            #pragma omp parallel for
            for (uint32_t i = 0; i < out_segment -> vec_len; i++)
                out_segment -> vec[i] = base_vertex_value;
        }
    }

    void exec_each(int round) {
        omp_set_num_threads(config -> cores);
        exec_each_initialize();
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " exec block";
            graphs[i] -> exec_each(round, i);
        }
    }

    void exec_diagonal(int round) {
        omp_set_num_threads(config -> cores);
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " exec diagonal";
            graphs[i] -> exec_diagonal(round);
        }
    }

    void pipeline_run(int round) {
        omp_set_num_threads(config -> cores);
        exec_each_initialize();
        std::string info_prefix = "round " + std::to_string(round) + " ";
        int graph_cnt = graphs.size();
        moodycamel::BlockingConcurrentQueue<int> exec_each_queue(graph_cnt);
        auto in_function = [&](comm_object<vwT> *in_segment){
            in_segment -> print(round);
            total_msg_size.fetch_add(in_segment -> caas_do(false, round, -1));
            in_segment -> print(round);
            for (auto i : in_segment -> related_graph_index) {
                exec_each_queue.enqueue(i);
            }
        };
        auto out_function = [&](comm_object<vwT> *out_segment){
            out_segment -> print(round);
            total_msg_size.fetch_add(out_segment -> caas_do(false, round, -1));
            out_segment -> print(round);
        };
        std::vector<std::thread> in_threads, out_threads;
        for (auto it = in_segments.begin(); it != in_segments.end(); it++) {
            in_threads.push_back(std::thread(in_function, it -> second));
        }
        std::thread exec_each_thread([&](){
            for (int i = 0; i < graph_cnt; i++) {
                int index;
                exec_each_queue.wait_dequeue(index);
                VLOG(1) << "graph " << index << " exec block";
                graphs[index] -> exec_each(round, index);
                graphs[index] -> out_segment -> finish++;
                if (graphs[index] -> out_segment -> finish == graphs[index] -> out_segment -> colocated_member) {
                    out_threads.push_back(std::thread(out_function, graphs[index] -> out_segment));
                }
            }
        });
        for (int i = 0; i < (int)in_threads.size(); i++) {
            in_threads[i].join();
        }
        exec_each_thread.join();
        for (int i = 0; i < (int)out_threads.size(); i++) {
            out_threads[i].join();
        }
        for (int i = 0; i < graph_cnt; i++) {
            VLOG(1) << "graph " << i << " exec diagonal";
            graphs[i] -> exec_diagonal(round);
        }
    }

    uint32_t vote(int round) {
        VLOG(1) << "vote";
        uint32_t msg = vote_object -> caas_do(
            config -> dynamic_invoke && !stateful && round != 1, 
            round, 
            in_segments.size() + out_segments.size() + 1
        );
        if (msg == CAAS_KILL_MESSAGE) {
            return CAAS_KILL_MESSAGE;
        }
        uint32_t activated = vote_object -> vec[0];
        vote_object -> vec[0] = 0;
        return activated;
    }

    void save_result(CAAS_SAVE_MODE save_mode, std::string local_dir) {
        switch (save_mode) {
             case CAAS_SAVE_MODE::NO_SAVE:
                break;
            case CAAS_SAVE_MODE::SAVE_LOCAL:
                for (int i = 0; i < (int)graphs.size(); i++) {
                    VLOG(1) << "graph " << i << " save result";
                    graphs[i] -> save_local();
                }
                break;
            case CAAS_SAVE_MODE::SAVE_S3:
                for (int i = 0; i < (int)graphs.size(); i++) {
                    VLOG(1) << "graph " << i << " save result";
                    graphs[i] -> save_s3(s_sdk);
                }
                break;
            default:
                LOG(FATAL) << "save mode " << (int)save_mode << " not implemented";
        }
    }

};

#endif
