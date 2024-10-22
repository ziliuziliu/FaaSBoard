#ifndef _GRAPH_SET_H
#define _GRAPH_SET_H

#include "graph.h"
#include "util/json.h"
#include "util/flags.h"
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

    graph_set(std::string graph_dir, uint8_t reduce_op, vwT base_vertex_value) {
        std::ifstream meta_file(graph_dir + "/graphs.meta");
        if (!meta_file.is_open()) {
            LOG(FATAL) << "could not open the file " << graph_dir + "/graphs.meta";
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
                item["edges"], reduce_op, base_vertex_value
            );
            newgraph -> read_csr(graph_dir + "/graph" + std::to_string(i) + ".csr");
            newgraph -> set_segment_connection(item["comm"]);
            #pragma omp critical 
            {
                graphs[i] = newgraph;
            }
        }
    }

    void connect(uint32_t request_id) {
        omp_set_num_threads(graphs.size());
        #pragma omp parallel for
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " connect to proxy";
            graphs[i] -> connect(request_id);
        }
    }

    void disconnect() {
        #pragma omp parallel for
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " disconnect to proxy";
        }
    }

    void begin(int round, auto vertex_initialize_func) {
        omp_set_num_threads(FLAGS_cores);
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " vertex initialize";
            graphs[i] -> begin(round, i, vertex_initialize_func);
        }
    }

    void in(int round) {
        omp_set_num_threads(graphs.size());
        #pragma omp parallel for
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " in broadcast";
            graphs[i] -> in(round, i);
        };
    }

    void out(int round) {
        omp_set_num_threads(graphs.size());
        #pragma omp parallel for
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " out reduce";
            graphs[i] -> out(round, i);
        }
    }

    void exec_each(int round, vwT vertex_initial_value, auto sparse_func, auto dense_func) {
        omp_set_num_threads(FLAGS_cores);
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " exec block";
            graphs[i] -> exec_each(round, i, vertex_initial_value, sparse_func, dense_func);
        }
    }

    void exec_diagonal(int round, auto reduce_func) {
        omp_set_num_threads(FLAGS_cores);
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " exec diagonal";
            graphs[i] -> exec_diagonal(round, i, reduce_func);
        }
    }

    void pipeline_run(int round, vwT vertex_initial_value, auto sparse_func, auto dense_func, auto reduce_func) {
        std::string info_prefix = "round " + std::to_string(round) + " ";
        int graph_cnt = graphs.size();
        moodycamel::BlockingConcurrentQueue<int> exec_each_queue(graph_cnt), exec_diagonal_queue(graph_cnt);
        auto in_function = [&](int index){
            graphs[index] -> in(round, index);
            exec_each_queue.enqueue(index);
        };
        auto out_function = [&](int index){
            graphs[index] -> out(round, index);
            exec_diagonal_queue.enqueue(index);
        };
        std::vector<std::thread> in_threads;
        for (int i = 0; i < graph_cnt; i++) {
            in_threads.push_back(std::thread(in_function, i));
        }
        std::thread exec_each_thread([&](){
            std::vector<std::thread> out_threads;
            for (int i = 0; i < graph_cnt; i++) {
                int index;
                exec_each_queue.wait_dequeue(index);
                VLOG(1) << "graph " << index << " exec block";
                graphs[index] -> exec_each(round, index, vertex_initial_value, sparse_func, dense_func);
                out_threads.push_back(std::thread(out_function, index));
            }
            for (int i = 0; i < (int)out_threads.size(); i++) {
                out_threads[i].join();
            }
        });
        std::thread exec_diagonal_thread([&](){
            for (int i = 0; i < graph_cnt; i++) {
                int index;
                exec_diagonal_queue.wait_dequeue(index);
                VLOG(1) << "graph " << index << " exec diagonal";
                graphs[index] -> exec_diagonal(round, index, reduce_func);
            }
        });
        for (int i = 0; i < graph_cnt; i++) {
            in_threads[i].join();
        }
        exec_each_thread.join();
        exec_diagonal_thread.join();
    }

    uint32_t vote() {
        uint32_t activated = 0;
        omp_set_num_threads(graphs.size());
        #pragma omp parallel for
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " vote";
            activated = graphs[i] -> vote();
        }
        return activated;
    }

    void save_result(std::string graph_dir) {
        for (int i = 0; i < (int)graphs.size(); i++) {
            VLOG(1) << "graph " << i << " save result";
            graphs[i] -> save_result(graph_dir + "/result" + std::to_string(i) + ".txt");
        }
    }

};

#endif
