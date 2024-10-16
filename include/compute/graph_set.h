#ifndef _GRAPH_SET_H
#define _GRAPH_SET_H

#include "graph.h"
#include "util/json.h"
#include "util/flags.h"

#include <fstream>
#include <vector>
#include <omp.h>

using json = nlohmann::json;

template<class vwT, class ewT>
class graph_set {

public:

    graph_meta meta;
    std::vector<graph<vwT, ewT> *> graphs;

    graph_set(std::string graph_dir, uint8_t reduce_op, vwT base_vertex_value) {
        std::ifstream meta_file(graph_dir + "/graphs.meta");
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
