#ifndef _BFS_H
#define _BFS_H

#include "compute/graph.h"
#include "compute/graph_set.h"
#include "communication/caas.h"
#include "util/timer.h"
#include "util/bitmap.h"
#include "util/types.h"
#include "util/atomic.h"
#include "util/log.h"

graph_set<uint32_t, empty> *graphs = nullptr;

void bfs(uint32_t request_id, uint32_t partition_id, uint32_t root, exec_config *config) {
    timer t;
    timer t2;
    double overall_idle_time = 0;
    t.start();
    t.tick("read graph");
    if (graphs == nullptr) {
        graphs = new graph_set<uint32_t, empty>(CAAS_REDUCE_OP::UP, 0xffffffff, config);
    } else {
        graphs -> update_config(config);
    }
    if (request_id == 0xffffffff) {
        return; // set 0xffffffff as the request id for keeping graph not evicted
    }
    graphs -> set_begin_func(
        [root](graph<uint32_t, empty> *g, uint32_t v){
            comm_object<uint32_t> *in_seg = g -> in_segment;
            if (v == root) {
                in_seg -> bm -> add(v - in_seg -> start_index);
                in_seg -> vec[v - in_seg -> start_index] = 0;
                return 1;
            } else {
                in_seg -> vec[v - in_seg -> start_index] = 0xffffffff;
                return 0;
            }
        }
    );
    graphs -> set_sparse_func(
        [](graph<uint32_t, empty> *g, int round, uint32_t src, uint32_t *out_dst, empty *out_w, uint32_t out_d) {
            comm_object<uint32_t> *in_seg = g -> in_segment, *out_seg = g -> out_segment;
            uint32_t *src_addr = in_seg -> vec + (src - in_seg -> start_index);
            for (uint32_t i = 0; i < out_d; i++) {
                uint32_t dst = out_dst[i];
                uint32_t *dst_addr = out_seg -> vec + (dst - out_seg -> start_index);
                if (*dst_addr == 0xffffffff && cas<uint32_t>(dst_addr, 0xffffffff, *src_addr + 1)) {
                    out_seg -> bm -> add(dst - out_seg -> start_index);
                }
            }
        }
    );
    graphs -> set_dense_func(
        [](graph<uint32_t, empty> *g, int round, uint32_t dst, uint32_t *in_src, empty *in_w, uint32_t in_d) {
            comm_object<uint32_t> *in_seg = g -> in_segment, *out_seg = g -> out_segment;
            uint32_t *dst_addr = out_seg -> vec + (dst - out_seg -> start_index);
            if (*dst_addr != 0xffffffff) {
                return;
            }
            for (uint32_t i = 0; i < in_d; i++) {
                uint32_t src = in_src[i];
                if (in_seg -> bm -> exist(src - in_seg -> start_index)) {
                    uint32_t *src_addr = in_seg -> vec + (src - in_seg -> start_index);
                    *dst_addr = *src_addr + 1;
                    out_seg -> bm -> add(dst - out_seg -> start_index);
                    break;
                }
            }
        }
    );
    graphs -> set_reduce_func(
        [](graph<uint32_t, empty> *g, int round, uint32_t v) {
            comm_object<uint32_t> *in_seg = g -> in_segment, *out_seg = g -> out_segment;
            uint32_t *in_addr = in_seg -> vec + (v - in_seg -> start_index);
            uint32_t *out_addr = out_seg -> vec + (v - out_seg -> start_index);
            if (*in_addr == 0xffffffff) {
                *in_addr = *out_addr;
                in_seg -> bm -> add(v - in_seg -> start_index);
            }
        }
    );
    graphs -> connect(request_id, partition_id);
    graphs -> begin(0);
    bool kill = false;
    if (!config -> no_pipeline) {
        timer t2;
        for (int round = 1; ; round++) {
            std::string info_prefix = "round " + std::to_string(round) + " ";
            t2.tick("vote");
            uint32_t activated = graphs -> vote(round);
            if (round == 1) {
                t.from_tick();
            } else {
                overall_idle_time += t2.from_tick();
            }
            if (activated == 0) {
                break;
            }
            if (activated == CAAS_KILL_MESSAGE) {
                kill = true;
                break;
            }
            t.tick(info_prefix);
            graphs -> pipeline_run(round);
            t.from_tick();
        }
    } else {
        for (int round = 1; ; round++) {
            std::string info_prefix = "round " + std::to_string(round) + " ";
            t2.tick("vote");
            uint32_t activated = graphs -> vote(round); 
            if (round == 1) {
                t.from_tick();
            }
            if (activated == 0) {
                break;
            }
            if (activated == CAAS_KILL_MESSAGE) {
                kill = true;
                break;
            }
            graphs -> in(round);
            graphs -> exec_each(round);
            graphs -> out(round);
            graphs -> exec_diagonal(round);
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
