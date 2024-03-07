#ifndef _RAW_GRAPH_H
#define _RAW_GRAPH_H

#include "graph.h"
#include "graph_set.h"
#include "partition.h"
#include "util/utils.h"
#include "util/print.h"

#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <cmath>
#include <cassert>
#include <omp.h>
#include <errno.h>

// TODO: add weight support

template <class T>
class raw_graph {

public:

    graph_meta meta;
    uint32_t *in_offset, *in_source, *out_offset, *out_dest;
    T *in_weight, *out_weight;

    raw_graph() {}

    raw_graph(uint32_t total_v, uint32_t total_e) {
        meta = graph_meta(total_v, total_e);
        in_offset = new uint32_t[meta.total_v + 1]{};
        in_source = new uint32_t[meta.total_e]{};
        out_offset = new uint32_t[meta.total_v + 1]{};
        out_dest = new uint32_t[meta.total_e]{};
    }

    void vertex_hash(uint32_t *mapping, uint32_t mx, uint32_t *edge_buffer, uint64_t edge_count) {
        for (uint32_t i = 1; i <= mx; i++)
            mapping[i] += mapping[i - 1];
        for (uint32_t i = 0; i <= mx; i++)
            mapping[i]--;
        #pragma omp parallel for
        for (uint64_t i = 0; i < edge_count; i++)
            edge_buffer[i] = mapping[edge_buffer[i]];
    }

    void from_txt(std::string path, bool hash = true) {
        print_log("start reading txt");
        FILE *txt_file = fopen(path.c_str(), "r");
        char *line = new char[100]; 
        size_t line_size = 0;
        uint32_t *mapping = new uint32_t[meta.total_v * 3];
        uint32_t *edge_buffer = new uint32_t[uint64_t(meta.total_e) * 2];
        uint64_t edge_buffer_count = 0;
        uint32_t mx = 0, x, y;
        while (getline(&line, &line_size, txt_file) > 0) {
            if (line[0] == '#') continue;
            parse_line(line, line_size, &x, &y);
            edge_buffer[edge_buffer_count++] = x;
            edge_buffer[edge_buffer_count++] = y;
            if (edge_buffer_count % 10000000 == 0)
                print_log("current size " + std::to_string(edge_buffer_count));
            mapping[x] = mapping[y] = 1;
            mx = std::max(mx, std::max(x, y));
        }
        fclose(txt_file);
        print_log("have read everything, size " + std::to_string(edge_buffer_count));
        print_log("start hashing");
        if (hash)
            vertex_hash(mapping, mx, edge_buffer, edge_buffer_count);
        print_log("start building csr");
        for (uint64_t i = 0; i < edge_buffer_count; i += 2) {
            uint32_t u = edge_buffer[i], v = edge_buffer[i + 1];
            in_offset[v + 1]++; 
            out_offset[u + 1]++;
        }
        for (uint32_t i = 1; i <= meta.total_v; i++) {
            in_offset[i] += in_offset[i - 1];
            out_offset[i] += out_offset[i - 1];
        }
        for (uint64_t i = 0; i < edge_buffer_count; i += 2) {
            uint32_t u = edge_buffer[i], v = edge_buffer[i + 1];
            in_source[in_offset[v]++] = u;
            out_dest[out_offset[u]++] = v;
        }
        for (uint32_t i = meta.total_v; i > 0; i--) {
            in_offset[i] = in_offset[i - 1];
            out_offset[i] = out_offset[i - 1];
        }
        in_offset[0] = out_offset[0] = 0;
        delete [] edge_buffer;
        delete [] mapping;
        print_log("end reading txt");
    }

    void read_csr(std::string path) {
        print_log("start reading csr");
        FILE *csr_file = fopen(path.c_str(), "rb");
        fread(in_offset, 4, meta.total_v + 1, csr_file);
        fread(in_source, 4, meta.total_e, csr_file);
        fread(out_offset, 4, meta.total_v + 1, csr_file);
        fread(out_dest, 4, meta.total_e, csr_file);
        fclose(csr_file);
        print_log("end reading csr");
    }
    
    void save_csr(std::string path) {
        print_log("start saving csr");
        FILE *csr_file = fopen(path.c_str(), "wb");
        fwrite(in_offset, 4, meta.total_v + 1, csr_file);
        fwrite(in_source, 4, meta.total_e, csr_file);
        fwrite(out_offset, 4, meta.total_v + 1, csr_file);
        fwrite(out_dest, 4, meta.total_e, csr_file);
        fclose(csr_file);
        print_log("end saving csr");
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
            print_log("left: " + std::to_string((int)left) + ", right: " + std::to_string((int)right));
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

    std::vector<graph_set<T> *> partition(partition_result result) {       
        std::vector<graph<T> *> subgraphs;
        for (auto block: result.blocks)
            subgraphs.push_back(new graph<T>(block, meta));
        sort(subgraphs.begin(), subgraphs.end(), graph<T>::column_order);
        #pragma omp parallel for
        for (int t = 0; t < result.column; t++) {
            for (uint32_t i = subgraphs[t * result.row] -> from_dest; i < subgraphs[t * result.row] -> to_dest; i++) {
                int ttp = 0;
                for (int tt = 0; tt < result.row; tt++)
                    subgraphs[t * result.row + tt] -> begin_add_edge(i, INCOMING);
                for (uint32_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
                    uint32_t source = in_source[j];
                    while (ttp < result.row && subgraphs[t * result.row + ttp] -> to_source <= source)
                        ttp++;
                    subgraphs[t * result.row + ttp] -> add_edge(source, i, INCOMING);
                }
            }
            for (int tt = 0; tt < result.row; tt++)
                subgraphs[t * result.row + tt] -> end_add_edge(INCOMING);
        }
        sort(subgraphs.begin(), subgraphs.end(), graph<T>::row_order);
        #pragma omp parallel for
        for (int t = 0; t < result.row; t++) {
            for (uint32_t i = subgraphs[t * result.column] -> from_source; i < subgraphs[t * result.column] -> to_source; i++) {
                int ttp = 0;
                for (int tt = 0; tt < result.column; tt++)
                    subgraphs[t * result.column + tt] -> begin_add_edge(i, OUTGOING);
                for (uint32_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                    uint32_t dest = out_dest[j];
                    while (ttp < result.column && subgraphs[t * result.column + ttp] -> to_dest <= dest)
                        ttp++;
                    subgraphs[t * result.column + ttp] -> add_edge(i, dest, OUTGOING);
                }
            }
            for (int tt = 0; tt < result.column; tt++)
                subgraphs[t * result.column + tt] -> end_add_edge(OUTGOING);
        }
        std::vector<graph_set<T> *> graphsets;
        #pragma omp parallel for
        for (int i = 0; i < (int)subgraphs.size(); i++) {
            graph_set<T> *graphset = new graph_set<T>(subgraphs[i], meta);
            #pragma omp critical
            {
                graphsets.push_back(graphset);
            }
        }
        return graphsets;
    }

    void print() {
        std::cout << "Total V: " << meta.total_v << std::endl;
        std::cout << "Total E: " << meta.total_e << std::endl;
        std::cout << "In Offset: ";
        print_array<uint32_t>(in_offset, meta.total_v + 1);
        std::cout << std::endl;
        std::cout << "In Source: ";
        print_array<uint32_t>(in_source, meta.total_e);
        std::cout << std::endl;
        std::cout << "Out Offset: ";
        print_array<uint32_t>(out_offset, meta.total_v + 1);
        std::cout << std::endl;
        std::cout << "Out Dest: ";
        print_array<uint32_t>(out_dest, meta.total_e);
        std::cout << std::endl;
    }

};

#endif
