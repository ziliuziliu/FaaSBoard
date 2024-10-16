#ifndef _RAW_GRAPH_H
#define _RAW_GRAPH_H

#include "graph.h"
#include "graph_set.h"
#include "partition.h"
#include "util/io.h"

#include <algorithm>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <cmath>
#include <cassert>
#include <omp.h>
#include <errno.h>

template <class ewT>
class raw_graph {

public:

    graph_meta meta;
    bool weighted;
    uint32_t *in_offset, *in_source, *out_offset, *out_dest;
    ewT *in_weight, *out_weight;

    raw_graph() {}

    raw_graph(uint32_t total_v, uint32_t total_e) {
        meta = graph_meta(total_v, total_e);
        weighted = !std::is_same<ewT, void *>::value;
        in_offset = new uint32_t[meta.total_v + 1]{};
        in_source = new uint32_t[meta.total_e]{};
        out_offset = new uint32_t[meta.total_v + 1]{};
        out_dest = new uint32_t[meta.total_e]{};
        if (weighted) {
            in_weight = new ewT[meta.total_e]{};
            out_weight = new ewT[meta.total_e]{};
        }
    }

    void read_txt(std::string path) {
        read_txt_util<ewT>(path, in_offset, in_source, out_offset, out_dest, in_weight, out_weight, 
            weighted, meta.total_v, meta.total_e);
    }

    void read_csr(std::string path) {
        read_csr_util<ewT>(path, in_offset, in_source, out_offset, out_dest, in_weight, out_weight, 
            weighted, meta.total_v, meta.total_v, meta.total_e);
    }

    void save_csr(std::string path) {
        save_csr_util<ewT>(path, in_offset, in_source, out_offset, out_dest, in_weight, out_weight, 
            weighted, meta.total_v, meta.total_v, meta.total_e);
    }

    partition_result row_partition(int total_block) {
        partition_result result(total_block, 1);
        uint64_t current_edges = 0, previous_from_source = 0;
        for (uint32_t i = 0; i < meta.total_v; i++) {
            if (current_edges * total_block >= meta.total_e) {
                result.add(previous_from_source, i, 0, meta.total_v, current_edges);
                previous_from_source = i;
                current_edges = 0;
            }
            current_edges += out_offset[i + 1] - out_offset[i];
        }
        result.add(previous_from_source, meta.total_v, 0, meta.total_v, current_edges);
        return result;
    }

    partition_result column_partition(int total_block) {
        partition_result result(1, total_block);
        uint64_t current_edges = 0, previous_from_dest = 0;
        for (uint32_t i = 0; i < meta.total_v; i++) {
            if (current_edges * total_block >= meta.total_e) {
                result.add(0, meta.total_v, previous_from_dest, i, current_edges);
                previous_from_dest = i;
                current_edges = 0;
            }
            current_edges += in_offset[i + 1] - in_offset[i];
        }
        result.add(0, meta.total_v, previous_from_dest, meta.total_v, current_edges);
        return result;
    }

    // require total_block to be square number
    partition_result mondriaan_partition_row_column(int total_block) {
        int cut = sqrt(total_block);
        partition_result row_result = row_partition(cut);
        partition_result final_result;
        #pragma omp parallel for
        for (int t = 0; t < (int)row_result.blocks.size(); t++) {
            partition_block block = row_result.blocks[t];
            uint32_t accum_edges = 0, previous_from_dest = 0;
            for (uint32_t i = 0; i < meta.total_v; i++) {
                if (accum_edges * cut >= block.edges) {
                    #pragma omp critical
                    {
                        final_result.add(block.from_source, block.to_source, previous_from_dest, i, accum_edges);
                        accum_edges = 0;
                        previous_from_dest = i;
                    }
                }
                uint32_t current_edges = 0;
                for (uint32_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
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
        return final_result;
    }

    // require total_block to be square number
    partition_result mondriaan_partition_column_row(int total_block) {
        int cut = sqrt(total_block);
        partition_result column_result = column_partition(cut);
        partition_result final_result;
        #pragma omp parallel for
        for (int t = 0; t < (int)column_result.blocks.size(); t++) {
            partition_block block = column_result.blocks[t];
            uint32_t accum_edges = 0, previous_from_source = 0;
            for (uint32_t i = 0; i < meta.total_v; i++) {
                if (accum_edges * cut >= block.edges) {
                    #pragma omp critical
                    {
                        final_result.add(previous_from_source, i, block.from_dest, block.to_dest, accum_edges);
                        accum_edges = 0;
                        previous_from_source = i;
                    }
                }
                uint32_t current_edges = 0;
                for (uint32_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
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
        return final_result;
    }

    partition_result generate_checkerboard_partition_from_cuts(int cut, std::vector<uint32_t> cuts) {
        partition_result result(cut, cut);
        #pragma omp parallel for
        for (int t = 0; t < cut; t++) {
            std::vector<uint32_t> block_edges(cut);
            for (uint32_t i = cuts[t]; i < cuts[t + 1]; i++) {
                int col_block_p = 0;
                for (uint32_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                    uint32_t dest = out_dest[j];
                    while (col_block_p < cut && dest >= cuts[col_block_p + 1])
                        col_block_p++;
                    block_edges[col_block_p]++;
                }
            }
            #pragma omp critical
            {
                for (int i = 0; i < cut; i++)
                    result.add(cuts[t], cuts[t + 1], cuts[i], cuts[i + 1], block_edges[i]);
            }
        }
        return result;
    }

    partition_result naive_checkerboard_partition(int cut) {
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
        return generate_checkerboard_partition_from_cuts(cut, cuts);
    }

    std::vector<uint32_t> generate_workload_limit_check_list(uint32_t left, uint32_t right) {
        int total_thread = omp_get_max_threads();
        std::vector<uint32_t> check_list;
        if (left == right || (int)(right - left - 1) < total_thread) {
            check_list.push_back(left + (right - left) / 2);
        } else {
            uint32_t step = (right - left - 1) / total_thread, p = left;
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
        uint32_t left = 1, right = meta.total_e;
        std::vector<uint32_t> result_cuts(cut + 1);
        while ((double)(right - left) / right >= 0.01) {
            VLOG(1) << "left: " << left << ", right: " << right;
            std::vector<uint32_t> check_list = generate_workload_limit_check_list(left, right);
            #pragma omp parallel for
            for (int t = 0; t < (int)check_list.size(); t++) {
                std::vector<uint32_t> cuts, workload(cut * 2 - 1), in_workload(cut), out_workload(cut);
                cuts.push_back(0);
                uint32_t workload_limit = check_list[t];
                bool plan_satisfy_limit = true;
                for (uint32_t i = 0; i < meta.total_v; i++) {
                    int cut_p = 0, block_p = -1, current_cut = cuts.size(), diagonal = 0;
                    std::fill(in_workload.begin(), in_workload.end(), 0);
                    std::fill(out_workload.begin(), out_workload.end(), 0);
                    for (uint32_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                        uint32_t dest = out_dest[j];
                        if (dest == i) diagonal = 1;
                        if (dest >= i) break;
                        while (cut_p < (int)cuts.size() && dest >= cuts[cut_p]) {
                            cut_p++;
                            block_p++;
                        }
                        out_workload[block_p]++;
                    }
                    cut_p = 0; 
                    block_p = -1;
                    for (uint32_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
                        uint32_t source = in_source[j];
                        if (source >= i) break;
                        while (cut_p < (int)cuts.size() && source >= cuts[cut_p]) {
                            cut_p++;
                            block_p++;
                        }
                        in_workload[block_p]++;
                    }
                    bool block_satisfy_limit = true;
                    for (int j = 0; j < current_cut - 1; j++)
                        if (workload[j] + out_workload[j] > workload_limit) {
                            block_satisfy_limit = false;
                            break;
                        }
                    for (int j = 0; j < current_cut - 1; j++)
                        if (workload[current_cut * 2 - 2 - j] + in_workload[j] > workload_limit) {
                            block_satisfy_limit = false;
                            break;
                        }
                    if (current_cut > 0) 
                        if (workload[current_cut - 1] + in_workload[current_cut - 1] + out_workload[current_cut - 1] + diagonal > workload_limit)
                            block_satisfy_limit = false;
                    if (!block_satisfy_limit) {
                        if ((int)cuts.size() == cut) {
                            plan_satisfy_limit = false;
                            break;
                        }
                        cuts.push_back(i);
                        current_cut++;
                        std::fill(workload.begin(), workload.end(), 0);
                    }
                    for (int j = 0; j < current_cut; j++)
                        workload[j] += out_workload[j];
                    for (int j = 0; j < current_cut; j++)
                        workload[current_cut * 2 - 2 - j] += in_workload[j];
                    workload[current_cut - 1] += diagonal;
                }
                #pragma omp critical 
                {
                    if (workload_limit >= left && workload_limit <= right) {
                        if (plan_satisfy_limit) {
                            for (int j = 0; j < (int)cuts.size(); j++)   
                                result_cuts[j] = cuts[j];
                            right = workload_limit - 1;
                        } else {
                            left = workload_limit + 1;
                        }
                    }
                }
            }
        }
        result_cuts[cut] = meta.total_v;
        return generate_checkerboard_partition_from_cuts(cut, result_cuts);
    }

    std::vector<graph_set<ewT> *> partition(partition_result result) {
        std::vector<graph<ewT> *> subgraphs;
        for (auto block: result.blocks)
            subgraphs.push_back(new graph<ewT>(block, meta));
        #pragma omp parallel for
        for (int t = 0; t < (int)subgraphs.size(); t++) {
            graph<ewT> *subgraph = subgraphs[t];
            for (uint32_t i = subgraph -> from_dest; i < subgraph -> to_dest; i++) {
                subgraph -> begin_add_edge(i, INCOMING);
                for (uint32_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
                    uint32_t source = in_source[j];
                    if (source >= subgraph -> from_source && source < subgraph -> to_source) {
                        if (!weighted) {
                            subgraph -> add_edge(source, i, INCOMING);
                        } else {
                            ewT weight = in_weight[j];
                            subgraph -> add_edge(source, i, weight, INCOMING);
                        }
                    }
                }
            }
            subgraph -> end_add_edge(INCOMING);
        }
        #pragma omp parallel for
        for (int t = 0; t < (int)subgraphs.size(); t++) {
            graph<ewT> *subgraph = subgraphs[t];
            for (uint32_t i = subgraph -> from_source; i < subgraph -> to_source; i++) {
                subgraph -> begin_add_edge(i, OUTGOING);
                for (uint32_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                    uint32_t dest = out_dest[j];
                    if (dest >= subgraph -> from_dest && dest < subgraph -> to_dest) {
                        if (!weighted) {
                            subgraph -> add_edge(i, dest, OUTGOING);
                        } else {
                            ewT weight = out_weight[j];
                            subgraph -> add_edge(i, dest, weight, OUTGOING);
                        }
                    }
                }
            }
            subgraph -> end_add_edge(OUTGOING);
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
        VLOG(2) << "In Offset: " << log_array<uint32_t>(in_offset, meta.total_v + 1).str();
        VLOG(2) << "In Source: " << log_array<uint32_t>(in_source, meta.total_e).str();
        VLOG(2) << "Out Offset: " << log_array<uint32_t>(out_offset, meta.total_v + 1).str();
        VLOG(2) << "Out Dest: " << log_array<uint32_t>(out_dest, meta.total_e).str();
    }

};

#endif
