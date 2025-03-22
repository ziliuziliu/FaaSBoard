#ifndef _CC_H
#define _CC_H

#include "compute/graph.h"
#include "compute/graph_set.h"
#include "communication/caas.h"
#include "util/timer.h"
#include "util/bitmap.h"
#include "util/types.h"
#include "util/atomic.h"
#include "util/log.h"

graph_set<uint32_t, empty> *graphs = nullptr;

void cc(uint32_t request_id, uint32_t partition_id, exec_config *config) {
    timer t;
    t.start();
    t.tick("read graph");
    if (graphs == nullptr) {
        graphs = new graph_set<uint32_t, empty>(CAAS_REDUCE_OP::MIN, 0xffffffff, config);
    } else {
        graphs -> update_config(config);
    }
    if (request_id == 0xffffffff) {
        return; // set 0xffffffff as the request id for keeping graph not evicted
    }
    graphs -> set_begin_func(
        [](graph<uint32_t, empty> *g, uint32_t v) {
            comm_object<uint32_t> *in_seg = g->in_segment;
            in_seg->bm->add(v - in_seg->start_index);
            in_seg->vec[v - in_seg->start_index] = v;
            return 1;
        }
    );

    graphs->set_sparse_func(
        [](graph<uint32_t, empty> *g, int round, uint32_t src, uint32_t *out_dst, empty *out_w, uint32_t out_d) {
            comm_object<uint32_t> *in_seg = g->in_segment, *out_seg = g->out_segment;
            for (uint32_t i = 0; i < out_d; i++) {
                uint32_t dst = out_dst[i];
                uint32_t *dst_addr = out_seg->vec + (dst - out_seg->start_index);
                uint32_t *src_addr = in_seg->vec + (src - in_seg->start_index);
                uint32_t new_cc_id = *src_addr;
                uint32_t old_value;
                do {
                    old_value = *dst_addr;
                    if (old_value <= new_cc_id) {
                        break;
                    }
                } while (!cas<uint32_t>(dst_addr, old_value, new_cc_id));
                if (old_value > new_cc_id) {
                    out_seg->bm->add(dst - out_seg->start_index);
                }
            }
        }
    );

    graphs->set_dense_func(
        [](graph<uint32_t, empty> *g, int round, uint32_t dst, uint32_t *in_src, empty *in_w, uint32_t in_d) {
            comm_object<uint32_t> *in_seg = g->in_segment, *out_seg = g->out_segment;
            uint32_t *dst_addr = out_seg->vec + (dst - out_seg->start_index);
            uint32_t min_cc_id = *dst_addr;
            for (uint32_t i = 0; i < in_d; i++) {
                uint32_t src = in_src[i];
                if (in_seg->bm->exist(src - in_seg->start_index)) {
                    uint32_t *src_addr = in_seg->vec + (src - in_seg->start_index);
                    min_cc_id = std::min(min_cc_id, *src_addr);
                }
            }
            if (*dst_addr > min_cc_id) {
                *dst_addr = min_cc_id;
                out_seg->bm->add(dst - out_seg->start_index);
            }
        }
    );

    graphs->set_reduce_func(
        [](graph<uint32_t, empty> *g, int round, uint32_t v) {
            comm_object<uint32_t> *in_seg = g->in_segment, *out_seg = g->out_segment;
            uint32_t *in_addr = in_seg->vec + (v - in_seg->start_index);
            uint32_t *out_addr = out_seg->vec + (v - out_seg->start_index);
            if (*in_addr > *out_addr) {
                *in_addr = *out_addr;
                in_seg->bm->add(v - in_seg->start_index);
            }
        }
    );

    graphs->connect(request_id, partition_id);
    graphs->begin(0);
    bool kill = false;

    if (!config->no_pipeline) {
        for (int round = 1;; round++) {
            std::string info_prefix = "round " + std::to_string(round) + " ";
            uint32_t activated = graphs->vote(round);
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
            t.tick(info_prefix);
            graphs->pipeline_run(round);
            t.from_tick();
        }
    } else {
        for (int round = 1;; round++) {
            std::string info_prefix = "round " + std::to_string(round) + " ";
            uint32_t activated = graphs->vote(round);
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
            graphs->in(round);
            graphs->exec_each(round);
            graphs->out(round);
            graphs->exec_diagonal(round);
        }
    }

    graphs -> disconnect();
    t.from_start("overall");
    if (!kill) {
        t.tick("save_result");
        graphs->save_result(config->save_mode, config->graph_dir);
        t.from_tick();
    }
}

#endif