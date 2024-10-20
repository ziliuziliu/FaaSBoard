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

void bfs(std::string graph_dir, uint32_t request_id, bool no_pipeline, uint32_t root) {
    timer t;
    t.start();
    t.tick("read graph");
    if (graphs == nullptr) {
        graphs = new graph_set<uint32_t, empty>(graph_dir, CAAS_UP, 0xffffffff);
    }
    t.from_tick();
    t.tick("connect");
    graphs -> connect(request_id);
    t.from_tick();
    t.tick("begin"); 
    graphs -> begin(
        0,
        [root](comm_object<uint32_t> *in_seg, uint32_t v){
        if (v == root) {
            in_seg -> bm -> add(v - in_seg -> start_index);
            in_seg -> vec[v - in_seg -> start_index] = 0;
            return 1;
        } else {
            in_seg -> vec[v - in_seg -> start_index] = 0xffffffff;
            return 0;
        }
    });
    t.from_tick();
    if (!no_pipeline) {
        for (int round = 1; ; round++) {
            std::string info_prefix = "round " + std::to_string(round) + " ";
            t.tick(info_prefix + "vote");
            uint32_t activated = graphs -> vote(); 
            t.from_tick();
            if (activated == 0) {
                break;
            }
            t.tick(info_prefix + "combine_run");
            graphs -> combine_run(
                round, -1, 
                [](comm_object<uint32_t> *in_seg, comm_object<uint32_t> *out_seg, uint32_t u, uint32_t v, empty w){
                    uint32_t *in_addr = in_seg -> vec + (u - in_seg -> start_index);
                    uint32_t *out_addr = out_seg -> vec + (v - out_seg -> start_index);
                    if (*out_addr == 0xffffffff && cas<uint32_t>(out_addr, 0xffffffff, *in_addr + 1)) {
                        out_seg -> bm -> add(v - out_seg -> start_index);
                    }
                },
                [](comm_object<uint32_t> *in_seg, comm_object<uint32_t> *out_seg, uint32_t u, uint32_t v, empty w){
                    uint32_t *in_addr = in_seg -> vec + (u - in_seg -> start_index);
                    uint32_t *out_addr = out_seg -> vec + (v - out_seg -> start_index);
                    if (*out_addr == 0xffffffff) {
                        *out_addr = *in_addr + 1;
                        out_seg -> bm -> add(v - out_seg -> start_index);
                    }
                },
                [](comm_object<uint32_t> *in_seg, comm_object<uint32_t> *out_seg, uint32_t v) {
                    uint32_t *in_addr = in_seg -> vec + (v - in_seg -> start_index);
                    uint32_t *out_addr = out_seg -> vec + (v - out_seg -> start_index);
                    if (*in_addr == 0xffffffff) {
                        *in_addr = *out_addr;
                        in_seg -> bm -> add(v - in_seg -> start_index);
                    }
                }
            );
            t.from_tick();
        }
    } else {
        for (int round = 1; ; round++) {
            std::string info_prefix = "round " + std::to_string(round) + " ";
            t.tick(info_prefix + "vote");
            uint32_t activated = graphs -> vote(); 
            t.from_tick();
            if (activated == 0) {
                break;
            }
            t.tick(info_prefix + "in");
            graphs -> in(round);
            t.from_tick();
            t.tick(info_prefix + "exec_each");
            graphs -> exec_each(
                round, -1, 
                [](comm_object<uint32_t> *in_seg, comm_object<uint32_t> *out_seg, uint32_t u, uint32_t v, empty w){
                    uint32_t *in_addr = in_seg -> vec + (u - in_seg -> start_index);
                    uint32_t *out_addr = out_seg -> vec + (v - out_seg -> start_index);
                    if (*out_addr == 0xffffffff && cas<uint32_t>(out_addr, 0xffffffff, *in_addr + 1)) {
                        out_seg -> bm -> add(v - out_seg -> start_index);
                    }
                },
                [](comm_object<uint32_t> *in_seg, comm_object<uint32_t> *out_seg, uint32_t u, uint32_t v, empty w){
                    uint32_t *in_addr = in_seg -> vec + (u - in_seg -> start_index);
                    uint32_t *out_addr = out_seg -> vec + (v - out_seg -> start_index);
                    if (*out_addr == 0xffffffff) {
                        *out_addr = *in_addr + 1;
                        out_seg -> bm -> add(v - out_seg -> start_index);
                    }
                }
            );
            t.from_tick();
            t.tick(info_prefix + "out");
            graphs -> out(round);
            t.from_tick();
            t.tick(info_prefix + "exec_diagonal");
            graphs -> exec_diagonal(
                round,
                [](comm_object<uint32_t> *in_seg, comm_object<uint32_t> *out_seg, uint32_t v) {
                    uint32_t *in_addr = in_seg -> vec + (v - in_seg -> start_index);
                    uint32_t *out_addr = out_seg -> vec + (v - out_seg -> start_index);
                    if (*in_addr == 0xffffffff) {
                        *in_addr = *out_addr;
                        in_seg -> bm -> add(v - in_seg -> start_index);
                    }
                }
            );
            t.from_tick();
        }
    }
    t.tick("disconnect");
    graphs -> disconnect();
    t.from_tick();
    t.from_start("overall");
    t.tick("save_result");
    graphs -> save_result(graph_dir);
    t.from_tick();
}

#endif