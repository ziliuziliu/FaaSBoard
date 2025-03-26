#ifndef _RAW_GRAPH_H
#define _RAW_GRAPH_H

#include "graph.h"
#include "graph_set.h"
#include "partition.h"
#include "util/io.h"
#include "util/timer.h"
#include "util/types.h"

#include <algorithm>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <cmath>
#include <utility>
#include <cassert>
#include <omp.h>
#include <errno.h>

template <class ewT>
class raw_graph {

public:

    graph_meta meta;
    bool weighted;
    uint64_t *in_offset, *out_offset;
    uint32_t *in_source, *out_dest, *in_degree, *out_degree;
    ewT *in_weight, *out_weight;

    raw_graph() {}

    raw_graph(uint32_t total_v, uint64_t total_e) {
        meta = graph_meta(total_v, total_e);
        weighted = !std::is_same<ewT, void *>::value;
        in_offset = new uint64_t[meta.total_v + 1]();
        in_source = new uint32_t[meta.total_e]();
        in_degree = new uint32_t[meta.total_v]();
        out_offset = new uint64_t[meta.total_v + 1]();
        out_dest = new uint32_t[meta.total_e]();
        out_degree = new uint32_t[meta.total_v]();
        if (weighted) {
            in_weight = new ewT[meta.total_e]();
            out_weight = new ewT[meta.total_e]();
        }
    }

    void read_txt(std::string path, bool undirected) {
        read_txt_util<ewT>(
            path, undirected,
            in_offset, in_source, in_weight, in_degree,
            out_offset, out_dest, out_weight, out_degree, 
            weighted, meta.total_v, meta.total_e
        );
    }

    void read_csr(std::string in_path, std::string out_path) {
        read_csr_util<ewT, uint64_t>(
            in_path, out_path, 
            in_offset, in_source, in_weight, in_degree,
            out_offset, out_dest, out_weight, out_degree, 
            weighted, false, false, true, meta.total_v, meta.total_v, meta.total_e
        );
    }

    void save_csr(std::string in_path, std::string out_path) {
        save_csr_util<ewT, uint64_t>(
            in_path, out_path, 
            in_offset, in_source, in_weight, in_degree,
            out_offset, out_dest, out_weight, out_degree, 
            weighted, meta.total_v, meta.total_v, meta.total_e
        );
    }

    partition_result row_partition(int total_block) {
        timer t;
        t.tick("partition time");
        partition_result result;
        uint64_t current_edges = 0, previous_from_source = 0;
        for (uint32_t i = 0; i < meta.total_v; i++) {
            if (i % 64 == 0 && current_edges * total_block >= meta.total_e) {
                result.add(previous_from_source, i, 0, meta.total_v, current_edges);
                previous_from_source = i;
                current_edges = 0;
            }
            current_edges += out_offset[i + 1] - out_offset[i];
        }
        result.add(previous_from_source, meta.total_v, 0, meta.total_v, current_edges);
        t.from_tick();
        return result;
    }

    partition_result column_partition(int total_block) {
        timer t;
        t.tick("partition time");
        partition_result result;
        uint64_t current_edges = 0, previous_from_dest = 0;
        for (uint32_t i = 0; i < meta.total_v; i++) {
            if (i % 64 == 0 && current_edges * total_block >= meta.total_e) {
                result.add(0, meta.total_v, previous_from_dest, i, current_edges);
                previous_from_dest = i;
                current_edges = 0;
            }
            current_edges += in_offset[i + 1] - in_offset[i];
        }
        result.add(0, meta.total_v, previous_from_dest, meta.total_v, current_edges);
        t.from_tick();
        return result;
    }

    partition_result mondriaan_partition_row_column(int total_block) {
        timer t;
        t.tick("partition time");
        std::pair<int,int> rank;
        rank = factorizeInt(total_block);
        int row_rank = rank.first, col_rank = rank.second;
        VLOG(1) << "row_rank = " << row_rank << "; col_rank = " << col_rank;
        partition_result row_result = row_partition(row_rank);
        row_result.print();
        partition_result final_result;
        #pragma omp parallel for
        for (int t = 0; t < (int)row_result.blocks.size(); t++) {
            partition_block block = row_result.blocks[t];
            uint32_t accum_edges = 0, previous_from_dest = 0;
            for (uint32_t i = 0; i < meta.total_v; i++) {
                if (i % 64 == 0 && accum_edges * col_rank >= block.edges) {
                    #pragma omp critical
                    {
                        final_result.add(block.from_source, block.to_source, previous_from_dest, i, accum_edges);
                        accum_edges = 0;
                        previous_from_dest = i;
                    }
                }
                uint32_t current_edges = 0;
                for (uint64_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
                    uint32_t source = in_source[j];
                    if (source >= block.from_source && source < block.to_source) {
                        current_edges++;
                    }
                }
                accum_edges += current_edges;
            }
            #pragma omp critical
            {
                final_result.add(block.from_source, block.to_source, previous_from_dest, meta.total_v, accum_edges);
            }
        }
        t.from_tick();
        return final_result;
    }

    partition_result mondriaan_partition_column_row(int total_block) {
        timer t;
        t.tick("partition time");
        std::pair<int,int> rank;
        rank = factorizeInt(total_block);
        int row_rank = rank.first, col_rank = rank.second;
        VLOG(1) << "row_rank = " << row_rank << "; col_rank = " << col_rank;
        partition_result column_result = row_partition(row_rank);
        partition_result final_result;
        #pragma omp parallel for
        for (int t = 0; t < (int)column_result.blocks.size(); t++) {
            partition_block block = column_result.blocks[t];
            uint32_t accum_edges = 0, previous_from_source = 0;
            for (uint32_t i = 0; i < meta.total_v; i++) {
                if (i % 64 == 0 && accum_edges * row_rank >= block.edges) {
                    #pragma omp critical
                    {
                        final_result.add(previous_from_source, i, block.from_dest, block.to_dest, accum_edges);
                        accum_edges = 0;
                        previous_from_source = i;
                    }
                }
                uint32_t current_edges = 0;
                for (uint64_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                    uint32_t dest = out_dest[j];
                    if (dest >= block.from_dest && dest < block.to_dest) {
                        current_edges++;
                    }
                }
                accum_edges += current_edges;
            }
            #pragma omp critical
            {
                final_result.add(previous_from_source, meta.total_v, block.from_dest, block.to_dest, accum_edges);
            }
        }
        t.from_tick();
        return final_result;
    }

    partition_result generate_checkerboard_partition_from_cuts(int cut, std::vector<uint32_t> cuts) {
        for (int i = 0; i < (int)cuts.size() - 1; i++) {
            cuts[i] = cuts[i] / 64 * 64;
        }
        VLOG(1) << "aligned cuts: " << log_array<uint32_t>(cuts.data(), uint64_t(cuts.size())).str();
        partition_result result;
        #pragma omp parallel for
        for (int t = 0; t < cut; t++) {
            std::vector<uint32_t> block_edges(cut);
            for (uint32_t i = cuts[t]; i < cuts[t + 1]; i++) {
                for (uint64_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                    uint32_t dest = out_dest[j];
                    for (int k = 0; k < cut; k++) {
                        if (dest >= cuts[k] && dest < cuts[k + 1]) {
                            block_edges[k]++;
                            break;
                        }
                    }
                }
            }
            #pragma omp critical
            {
                for (int i = 0; i < cut; i++) {
                    result.add(cuts[t], cuts[t + 1], cuts[i], cuts[i + 1], block_edges[i]);
                }
            }
        }
        return result;
    }

    partition_result naive_checkerboard_partition(int cut) {
        timer t;
        t.tick("partition time");
        std::vector<uint32_t> cuts;
        uint32_t offset = 0;
        for (int i = 0; i < (int)meta.total_v % cut; i++) {
            cuts.push_back(offset);
            offset += meta.total_v / cut + 1;
        }
        for (int i = (int)meta.total_v % cut; i < cut; i++) {
            cuts.push_back(offset);
            offset += meta.total_v / cut;
        }
        cuts.push_back(offset);
        t.from_tick();
        return generate_checkerboard_partition_from_cuts(cut, cuts);
    }

    std::vector<uint64_t> generate_workload_limit_check_list(uint64_t left, uint64_t right) {
        int total_thread = omp_get_max_threads();
        std::vector<uint64_t> check_list;
        if (left == right || (right - left - 1) < uint64_t(total_thread)) {
            check_list.push_back(left + (right - left) / 2);
        } else {
            uint64_t step = (right - left - 1) / total_thread, p = left;
            for (uint32_t i = 0; i < (right - left - 1) % total_thread; i++) {
                p += step + 1;
                check_list.push_back(p);
            }
            for (uint32_t i = (right - left - 1) % total_thread; i < (uint32_t)total_thread; i++) {
                p += step;
                check_list.push_back(p);
            }
        }
        return check_list;
    }

    partition_result checkerboard_partition(int cut) {
        if (cut == 1) {
            std::vector<uint32_t> cuts = {0, meta.total_v};
            return generate_checkerboard_partition_from_cuts(cut, cuts);
        }
        timer t;
        t.tick("partition time");
        uint64_t left = 0, right = meta.total_e + meta.total_v * 2 * COMM_COMP_RATIO;
        std::vector<uint32_t> result_cuts(cut + 1, 0);
        while ((double)(right - left) / right >= 0.01 || result_cuts[cut - 1] == 0) {
            VLOG(1) << "left: " << left << ", right: " << right;
            std::vector<uint64_t> check_list = generate_workload_limit_check_list(left, right);
            #pragma omp parallel for
            for (int t = 0; t < (int)check_list.size(); t++) {
                std::vector<uint32_t> cuts, in_workload(cut), out_workload(cut);
                std::vector<uint64_t> workload(cut * 2 - 1);
                cuts.push_back(0);
                uint64_t workload_limit = check_list[t];
                bool plan_satisfy_limit = true;
                for (uint32_t i = 0; i < meta.total_v; i++) {
                    int current_cut = cuts.size(), diagonal = 0;
                    std::fill(in_workload.begin(), in_workload.end(), 0);
                    std::fill(out_workload.begin(), out_workload.end(), 0);
                    for (uint64_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                        uint32_t dest = out_dest[j];
                        if (dest == i) diagonal = 1;
                        if (dest >= i) continue;
                        for (int k = 0; k < current_cut; k++) {
                            if (k == current_cut - 1) {
                                out_workload[k]++;
                                break;
                            } else if (cuts[k] <= dest && dest < cuts[k + 1]) {
                                out_workload[k]++;
                                break;
                            }
                        }
                    }
                    for (uint64_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
                        uint32_t source = in_source[j];
                        if (source >= i) continue;
                        for (int k = 0; k < current_cut; k++) {
                            if (k == current_cut - 1) {
                                in_workload[k]++;
                                break;
                            } else if (cuts[k] <= source && source < cuts[k + 1]) {
                                in_workload[k]++;
                                break;
                            }
                        }
                    }
                    for (int j = 0; j < current_cut; j++) {
                        in_workload[j] += COMM_COMP_RATIO;
                        out_workload[j] += COMM_COMP_RATIO;
                    }
                    bool block_satisfy_limit = true;
                    for (int j = 0; j < current_cut - 1; j++)
                        if (workload[j] + uint64_t(out_workload[j]) > workload_limit) {
                            block_satisfy_limit = false;
                            break;
                        }
                    for (int j = 0; j < current_cut - 1; j++)
                        if (workload[current_cut * 2 - 2 - j] + uint64_t(in_workload[j]) > workload_limit) {
                            block_satisfy_limit = false;
                            break;
                        }
                    if (current_cut > 0) 
                        if (workload[current_cut - 1] + uint64_t(in_workload[current_cut - 1] + out_workload[current_cut - 1] + diagonal) > workload_limit)
                            block_satisfy_limit = false;
                    if (!block_satisfy_limit) {
                        cuts.push_back(i);
                        if ((int)cuts.size() == cut + 1) {
                            plan_satisfy_limit = false;
                            break;
                        }
                        current_cut++;
                        for (int j = 0; j < current_cut - 1; j++) {
                            workload[j] = (uint64_t)(cuts[j + 1] - cuts[j]) * COMM_COMP_RATIO;
                        }
                        for (int j = 0; j < current_cut - 1; j++) {
                            workload[current_cut * 2 - 2 - j] = (uint64_t)(cuts[j + 1] - cuts[j]) * COMM_COMP_RATIO;
                        }
                        workload[current_cut - 1] = 0;
                    }
                    for (int j = 0; j < current_cut; j++) {
                        workload[j] += uint64_t(out_workload[j]);
                    }
                    for (int j = 0; j < current_cut; j++) {
                        workload[current_cut * 2 - 2 - j] += uint64_t(in_workload[j]);
                    }
                    workload[current_cut - 1] += uint64_t(diagonal);
                }
                #pragma omp critical 
                {
                    if (plan_satisfy_limit) {
                        if (workload_limit >= left && workload_limit <= right) {
                            for (int j = 0; j < (int)cuts.size(); j++)   
                                result_cuts[j] = cuts[j];
                            right = workload_limit - 1;
                        }
                    } else {
                        if (workload_limit >= left && workload_limit <= right) {
                            left = workload_limit + 1;
                        }
                    }
                }
            }
        }
        result_cuts[cut] = meta.total_v;
        t.from_tick();
        return generate_checkerboard_partition_from_cuts(cut, result_cuts);
    }

    std::vector<graph_set<ewT> *> partition(partition_result result, bool clean) {
        std::vector<graph<ewT> *> subgraphs;
        for (auto block: result.blocks){
            if (block.edges != 0 || block.root() || !clean) {
                subgraphs.push_back(new graph<ewT>(block, meta));
            }
        }
        #pragma omp parallel for
        for (int t = 0; t < (int)subgraphs.size(); t++) {
            graph<ewT> *subgraph = subgraphs[t];
            for (uint32_t i = subgraph -> from_dest; i < subgraph -> to_dest; i++) {
                subgraph -> begin_add_edge(i, EDGE_DIRECTION::INCOMING);
                for (uint64_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
                    uint32_t source = in_source[j];
                    if (source >= subgraph -> from_source && source < subgraph -> to_source) {
                        if (!weighted) {
                            subgraph -> add_edge(source, i, EDGE_DIRECTION::INCOMING);
                        } else {
                            ewT weight = in_weight[j];
                            subgraph -> add_edge(source, i, weight, EDGE_DIRECTION::INCOMING);
                        }
                    }
                }
            }
            subgraph -> end_add_edge(EDGE_DIRECTION::INCOMING);
        }
        #pragma omp parallel for
        for (int t = 0; t < (int)subgraphs.size(); t++) {
            graph<ewT> *subgraph = subgraphs[t];
            for (uint32_t i = subgraph -> from_source; i < subgraph -> to_source; i++) {
                subgraph -> begin_add_edge(i, EDGE_DIRECTION::OUTGOING);
                for (uint64_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                    uint32_t dest = out_dest[j];
                    if (dest >= subgraph -> from_dest && dest < subgraph -> to_dest) {
                        if (!weighted) {
                            subgraph -> add_edge(i, dest, EDGE_DIRECTION::OUTGOING);
                        } else {
                            ewT weight = out_weight[j];
                            subgraph -> add_edge(i, dest, weight, EDGE_DIRECTION::OUTGOING);
                        }
                    }
                }
            }
            subgraph -> end_add_edge(EDGE_DIRECTION::OUTGOING);
        }
        for (int i = 0; i < (int)subgraphs.size(); i++) {
            subgraphs[i] -> set_in_degree(in_degree + subgraphs[i] -> from_dest);
            subgraphs[i] -> set_out_degree(out_degree + subgraphs[i] -> from_source);
        }
        for (int i = 0; i < (int)subgraphs.size(); i++) {
            subgraphs[i] -> check();
        }
        std::vector<graph_set<ewT> *> graphsets(subgraphs.size(), nullptr);
        #pragma omp parallel for
        for (int i = 0; i < (int)subgraphs.size(); i++) {
            graph_set<ewT> *graphset = new graph_set<ewT>(subgraphs[i], meta);
            #pragma omp critical
            {
                graphsets[i] = graphset;
            }
        }
        return graphsets;
    }

    void print() {
        VLOG(1) << "Total V: " << meta.total_v;
        VLOG(1) << "Total E: " << meta.total_e;
        VLOG(2) << "In Offset: " << log_array<uint64_t>(in_offset, uint64_t(meta.total_v + 1)).str();
        VLOG(2) << "In Source: " << log_array<uint32_t>(in_source, meta.total_e).str();
        VLOG(2) << "Out Offset: " << log_array<uint64_t>(out_offset, uint64_t(meta.total_v + 1)).str();
        VLOG(2) << "Out Dest: " << log_array<uint32_t>(out_dest, meta.total_e).str();
    }

};

#endif
