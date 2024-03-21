#ifndef _GRAPH_SET_H
#define _GRAPH_SET_H

#include <algorithm>
#include <stdexcept>
#include <cmath>

#include "graph.h"

template<class T>
class graph_set {

public:

    graph_meta meta;
    std::vector<graph<T> *> graphs;
    bitmap *recv_map, *send_map, *manage_map;
    uint32_t edges;

    graph_set() {}

    graph_set(graph<T> *subgraph, graph_meta meta): meta(meta), edges(subgraph -> edges) {
        graphs = std::vector<graph<T> *>{subgraph};
        recv_map = new bitmap(meta.total_v);
        send_map = new bitmap(meta.total_v);
        manage_map = new bitmap(meta.total_v);
        for (uint32_t v = subgraph -> from_source; v < subgraph -> to_source; v++) {
            std::tuple<uint32_t *, uint32_t> out_edge_tuple = subgraph -> out_edge(v);
            uint32_t *out_edge = std::get<0>(out_edge_tuple);
            uint32_t out_edge_cnt = std::get<1>(out_edge_tuple);
            if (out_edge_cnt == 0) continue;
            recv_map -> add(v);
            for (uint32_t i = 0; i < out_edge_cnt; i++)
                send_map -> add(out_edge[i]);       
        }
        uint32_t manage_range_left = std::max(subgraph -> from_source, subgraph -> from_dest);
        uint32_t manage_range_right = std::min(subgraph -> to_source, subgraph -> to_dest);
        for (uint32_t i = manage_range_left; i < manage_range_right; i++)
            manage_map -> add(i);
    }

    graph_set(graph_set<T> *s1, graph_set<T> *s2) {
        meta = s1 -> meta;
        graphs = std::vector<graph<T> *>();
        graphs.insert(graphs.end(), s1 -> graphs.begin(), s1 -> graphs.end());
        graphs.insert(graphs.end(), s2 -> graphs.begin(), s2 -> graphs.end());
        recv_map = s1 -> recv_map -> OR(s2 -> recv_map);
        send_map = s1 -> send_map -> OR(s2 -> send_map);
        manage_map = s1 -> manage_map -> OR(s2 -> manage_map);
        edges = s1 -> edges + s2 -> edges;
    }

    ~graph_set() {
        delete send_map;
        delete recv_map;
        delete manage_map;
    }

    static uint32_t gain(graph_set<T> *s1, graph_set<T> *s2) {
        bitmap *ret1 = s1 -> recv_map -> AND(s2 -> recv_map), *ret2 = s1 -> send_map -> AND(s2 -> send_map);
        uint32_t size = ret1 -> size + ret2 -> size;
        delete ret1;
        delete ret2;
        return size;
    }

    static std::vector<graph_set<T> *> cycle(std::vector<graph_set<T> *> graphsets, int total_block) {
        int step = sqrt(total_block);
        std::vector<graph_set<T> *> cycle_graphsets(total_block, nullptr);
        for (int i = 0; i < step; i++)
            for (int j = 0; j < step; j++) {
                #pragma omp parallel for
                for (int t = 0; t < total_block; t++) {
                    int px = i * step + t / step, py = j * step + t % step;
                    graph_set<T> *add = graphsets[px * total_block + py];
                    graph_set<T> *old = cycle_graphsets[t];
                    if (old == nullptr)
                        cycle_graphsets[t] = add;
                    else {
                        cycle_graphsets[t] = new graph_set<T>(old, add);
                        delete old;
                    }
                }
            }
        return cycle_graphsets;
    }

    static std::vector<graph_set<T> *> stagger(std::vector<graph_set<T> *> graphsets, int total_block) {
        int step = sqrt(total_block);
        std::vector<graph_set<T> *> stagger_graphsets(total_block, nullptr);
        for (int i = 0; i < step; i++)
            for (int j = 0; j < step; j++) {
                #pragma omp parallel for
                for (int t = 0; t < total_block; t++) {
                    int px = i * step + t / step, py = j * step + t % step;
                    int target = (t + i * step) % total_block;
                    graph_set<T> *add = graphsets[px * total_block + py];
                    graph_set<T> *old = stagger_graphsets[target];
                    if (old == nullptr)
                        stagger_graphsets[target] = add;
                    else {
                        stagger_graphsets[target] = new graph_set<T>(old, add);
                        delete old;
                    }
                }
            }
        return stagger_graphsets;
    }

    static std::vector<graph_set<T> *> binpack(std::vector<graph_set<T> *> graphsets, int total_block, double balance_ratio_limit) {
        int graphset_limit = total_block;
        uint32_t workload_limit = (uint32_t)((double)graphsets[0] -> meta.total_e / graphset_limit * balance_ratio_limit);
        std::sort(graphsets.begin(), graphsets.end(), [](graph_set<T> *a, graph_set<T> *b) {
            if (a -> edges > b -> edges)
                return true;
            return false;
        });
        if (graphsets[0] -> edges > workload_limit)
            throw std::runtime_error("no satisfying plan");
        std::vector<bool> deleted(graphsets.size());
        for (int i = graphset_limit; i < (int)graphsets.size(); i++) {
            graph_set<T> *graphsets_a, *graphsets_b;
            uint32_t max_gain = 0;
            int place_a = -1, place_b = -1;
            #pragma omp parallel for
            for (int j = 0; j < (int)graphsets.size(); j++)
                for (int k = j + 1; k < (int)graphsets.size(); k++) {
                    if (deleted[j] || deleted[k] || graphsets[j] -> edges + graphsets[k] -> edges > workload_limit)
                        continue;
                    uint32_t gain = graph_set<T>::gain(graphsets[j], graphsets[k]);
                    #pragma omp critical
                    {
                        if (gain >= max_gain) {
                            max_gain = gain;
                            graphsets_a = graphsets[j];
                            graphsets_b = graphsets[k];
                            place_a = j;
                            place_b = k;
                        }
                    }
                }
            if (place_a == -1 && place_b == -1)
                throw std::runtime_error("no satisfying plan");
            graphsets[place_a] = new graph_set<T>(graphsets_a, graphsets_b);
            deleted[place_b] = true;
        }
        std::vector<graph_set<T> *> new_graphsets;
        for (int i = 0; i < (int)graphsets.size(); i++) {
            if (!deleted[i])
                new_graphsets.push_back(graphsets[i]);
        }
        return new_graphsets;
    }

    static void simulation(std::vector<graph_set<T> *> graphsets) {
        std::vector<uint32_t> works;
        uint32_t total_work = 0, min_work, max_work;
        double avg_work;
        int total_block = graphsets.size();
        for (auto graphset: graphsets) {
            total_work += graphset -> edges;
            works.push_back(graphset -> edges);
        }
        sort(works.begin(), works.end());
        avg_work = (double)total_work / total_block;
        min_work = works[0];
        max_work = works[total_block - 1];
        std::cout << "Total Work: " << total_work << ", Avg Work: " << avg_work;
        std::cout << ", Max Work: " << max_work << ", Min Work: " << min_work;
        std::cout << ", Ratio: " << (double)max_work / avg_work << std::endl;
        uint64_t total_comm = 0, max_comm, min_comm;
        double avg_comm;
        std::vector<uint64_t> comms;
        for (auto graphset: graphsets) {
            bitmap *ret1, *ret2, *ret3;
            ret1 = graphset -> manage_map -> NOT();
            ret2 = graphset -> recv_map -> AND(ret1);
            ret3 = graphset -> send_map -> AND(ret1);
            total_comm += ret2 -> size + ret3 -> size;
            comms.push_back(ret2 -> size + ret3 -> size);
            delete ret1;
            delete ret2;
            delete ret3;
        }
        sort(comms.begin(), comms.end());
        avg_comm = (double)total_comm / total_block;
        min_comm = comms[0];
        max_comm = comms[total_block - 1];
        std::cout << "Total Comm: " << total_comm << ", Avg Comm: " << avg_comm;
        std::cout << ", Max Comm: " << max_comm << ", Min Comm: " << min_comm << std::endl;
    }

    void print() {
        std::cout << "GraphSet: " << std::endl;
        for (auto graph: graphs)
            graph -> print();
    }

};

#endif
