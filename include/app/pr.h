#ifndef _PR_H
#define _PR_H

#include "compute/graph.h"
#include "compute/graph_set.h"
#include "communication/caas.h"
#include "util/timer.h"
#include "util/bitmap.h"
#include "util/types.h"
#include "util/atomic.h"
#include "util/log.h"
#include "util/flags.h"

graph_set<float, empty> *graphs = nullptr;

void pagerank(uint32_t request_id, uint32_t partition_id, int iterations, exec_config *config) {
    timer t;
    timer t2;
    double overall_idle_time = 0;
    t.start();
    t.tick("read graph");
    if (graphs == nullptr) {
        graphs = new graph_set<float, empty>(CAAS_REDUCE_OP::ADD, 0.0, config);
    } else {
        graphs -> update_config(config);
    }
    if (request_id == 0xffffffff) {
        return; // set 0xffffffff as the request id for keeping graph not evicted
    }
    graphs -> set_begin_func(
        [](graph<float, empty> *g, uint32_t v){
            comm_object<float> *in_seg = g -> in_segment;
            uint32_t out_degree = g -> out_degree[v - g -> from_source];
            in_seg -> bm -> add(v - in_seg -> start_index);
            in_seg -> vec[v - in_seg -> start_index] = out_degree > 0 ? 1.0 / out_degree : 1.0;
            return 1;
        }
    );
    graphs -> set_sparse_func(
        [](graph<float, empty> *g, int round, uint32_t src, uint32_t *out_dst, empty *out_w, uint32_t out_d) {
            comm_object<float> *in_seg = g -> in_segment, *out_seg = g -> out_segment;
            float *src_addr = in_seg -> vec + (src - in_seg -> start_index);
            for (uint32_t i = 0; i < out_d; i++) {
                uint32_t dst = out_dst[i];
                float *dst_addr = out_seg -> vec + (dst - out_seg -> start_index);
                while (!cas<float>(dst_addr, *dst_addr, *dst_addr + *src_addr));
            }
            if (src == in_seg -> start_index) {
                out_seg -> bm -> fill();
            }
        }
    );
    graphs -> set_dense_func(
        [](graph<float, empty> *g, int round, uint32_t dst, uint32_t *in_src, empty *in_w, uint32_t in_d) {
            comm_object<float> *in_seg = g -> in_segment, *out_seg = g -> out_segment;
            float *dst_addr = out_seg -> vec + (dst - out_seg -> start_index);
            for (uint32_t i = 0; i < in_d; i++) {
                uint32_t src = in_src[i];
                float *src_addr = in_seg -> vec + (src - in_seg -> start_index);
                *dst_addr += *src_addr;
            }
            if (dst == out_seg -> start_index) {
                out_seg -> bm -> fill();
            }
        }
    );
    graphs -> set_reduce_func(
        [iterations](graph<float, empty> *g, int round, uint32_t v) {
            comm_object<float> *in_seg = g -> in_segment, *out_seg = g -> out_segment;
            float *in_addr = in_seg -> vec + (v - in_seg -> start_index);
            float *out_addr = out_seg -> vec + (v - out_seg -> start_index);
            *in_addr = 0.15 + 0.85 * *out_addr;
            if (round != iterations) {
                uint32_t out_degree = g -> out_degree[v - g -> from_source];
                *in_addr = out_degree > 0 ? *in_addr / out_degree : *in_addr;
            }
            if (v == in_seg -> start_index) {
                in_seg -> bm -> fill();
            }
        }
    );
    graphs -> connect(request_id, partition_id);
    graphs -> begin(0);
    bool kill = false;
    if (!config -> no_pipeline) {
        for (int round = 1; round <= iterations; round++) {
            std::string info_prefix = "round " + std::to_string(round) + " ";
            t2.tick("vote");
            uint32_t activated = graphs -> vote(round);
            if (round == 1) {
                t.from_tick();
            } else {
                overall_idle_time += t2.from_tick();
            }
            if (activated == CAAS_KILL_MESSAGE) {
                kill = true;
                break;
            }
            t2.tick(info_prefix + "run");
            graphs -> pipeline_run(round);
            t2.from_tick();
        }
    } else {
        for (int round = 1; round <= iterations; round++) {
            std::string info_prefix = "round " + std::to_string(round) + " ";
            t2.tick("vote");
            uint32_t activated = graphs -> vote(round); 
            if (round == 1) {
                t.from_tick();
            } else {
                overall_idle_time += t2.from_tick();
            }
            if (activated == CAAS_KILL_MESSAGE) {
                kill = true;
                break;
            }
            t2.tick(info_prefix + "in");
            graphs -> in(round);
            t2.from_tick();
            t2.tick(info_prefix + "exec_each");
            graphs -> exec_each(round);
            t2.from_tick();
            t2.tick(info_prefix + "out");
            graphs -> out(round);
            t2.from_tick();
            t2.tick(info_prefix + "exec_diagonal");
            graphs -> exec_diagonal(round);
            t2.from_tick();
        }
    }
    graphs -> disconnect();
    double overall_time = t.from_start("overall");
    VLOG(1) << "total_msg_size: " << (double)graphs -> total_msg_size.load() / 1024 / 1024 << " MB";
    VLOG(1) << "overall_time: " << (double)overall_time << " s";
    VLOG(1) << "overall_idle_time: " << (double)overall_idle_time << " s";
    if (!kill) {
        t.tick("save_result");
        graphs -> save_result(config -> save_mode, config -> graph_dir);
        t.from_tick();
    }
}

#endif