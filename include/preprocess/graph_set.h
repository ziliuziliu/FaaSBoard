#ifndef _GRAPH_SET_H
#define _GRAPH_SET_H

#include "graph.h"
#include "util/json.h"
#include "util/flags.h"

#include <set>
#include <algorithm>
#include <stdexcept>
#include <cmath>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;
using json = nlohmann::json;

template<class ewT>
class graph_set {

public:

    graph_meta meta;
    std::vector<graph<ewT> *> graphs;
    bitmap *recv_map, *send_map, *manage_map;
    uint32_t edges;

    graph_set() {}

    graph_set(graph<ewT> *subgraph, graph_meta meta): meta(meta), edges(subgraph -> edges) {
        graphs = std::vector<graph<ewT> *>{subgraph};
        recv_map = new bitmap(meta.total_v);
        send_map = new bitmap(meta.total_v);
        manage_map = new bitmap(meta.total_v);
        for (uint32_t v = subgraph -> from_source; v < subgraph -> to_source; v++) {
            std::tuple<uint32_t *, ewT *, uint32_t> out_edge_tuple = subgraph -> out_edge(v);
            uint32_t *out_edge = std::get<0>(out_edge_tuple);
            uint32_t out_edge_cnt = std::get<2>(out_edge_tuple);
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

    graph_set(graph_set<ewT> *s1, graph_set<ewT> *s2) {
        meta = s1 -> meta;
        graphs = std::vector<graph<ewT> *>();
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

    static uint32_t gain(graph_set<ewT> *s1, graph_set<ewT> *s2) {
        bitmap *ret1 = s1 -> recv_map -> AND(s2 -> recv_map), *ret2 = s1 -> send_map -> AND(s2 -> send_map);
        uint32_t size = ret1 -> get_size() + ret2 -> get_size();
        delete ret1;
        delete ret2;
        return size;
    }

    static std::vector<graph_set<ewT> *> cycle(std::vector<graph_set<ewT> *> graphsets, int total_block) {
        int step = sqrt(total_block);
        std::vector<graph_set<ewT> *> cycle_graphsets(total_block, nullptr);
        for (int i = 0; i < step; i++)
            for (int j = 0; j < step; j++) {
                #pragma omp parallel for
                for (int t = 0; t < total_block; t++) {
                    int px = i * step + t / step, py = j * step + t % step;
                    graph_set<ewT> *add = graphsets[px * total_block + py];
                    graph_set<ewT> *old = cycle_graphsets[t];
                    if (old == nullptr) {
                        cycle_graphsets[t] = add;
                    }
                    else {
                        cycle_graphsets[t] = new graph_set<ewT>(old, add);
                        delete old;
                    }
                }
            }
        return cycle_graphsets;
    }

    static std::vector<graph_set<ewT> *> stagger(std::vector<graph_set<ewT> *> graphsets, int total_block) {
        int step = sqrt(total_block);
        std::vector<graph_set<ewT> *> stagger_graphsets(total_block, nullptr);
        for (int i = 0; i < step; i++)
            for (int j = 0; j < step; j++) {
                #pragma omp parallel for
                for (int t = 0; t < total_block; t++) {
                    int px = i * step + t / step, py = j * step + t % step;
                    int target = (t + i * step) % total_block;
                    graph_set<ewT> *add = graphsets[px * total_block + py];
                    graph_set<ewT> *old = stagger_graphsets[target];
                    if (old == nullptr)
                        stagger_graphsets[target] = add;
                    else {
                        stagger_graphsets[target] = new graph_set<ewT>(old, add);
                        delete old;
                    }
                }
            }
        return stagger_graphsets;
    }

    static std::vector<graph_set<ewT> *> binpack(std::vector<graph_set<ewT> *> graphsets, int total_block, double balance_ratio_limit) {
        int graphset_limit = total_block;
        uint32_t workload_limit = (uint32_t)((double)graphsets[0] -> meta.total_e / graphset_limit * balance_ratio_limit);
        std::sort(graphsets.begin(), graphsets.end(), [](graph_set<ewT> *a, graph_set<ewT> *b) {
            if (a -> edges > b -> edges)
                return true;
            return false;
        });
        if (graphsets[0] -> edges > workload_limit)
            throw std::runtime_error("no satisfying plan");
        std::vector<bool> deleted(graphsets.size());
        for (int i = graphset_limit; i < (int)graphsets.size(); i++) {
            graph_set<ewT> *graphsets_a, *graphsets_b;
            uint32_t max_gain = 0;
            int place_a = -1, place_b = -1;
            #pragma omp parallel for
            for (int j = 0; j < (int)graphsets.size(); j++)
                for (int k = j + 1; k < (int)graphsets.size(); k++) {
                    if (deleted[j] || deleted[k] || graphsets[j] -> edges + graphsets[k] -> edges > workload_limit) {
                        continue;
                    }
                    uint32_t gain = graph_set<ewT>::gain(graphsets[j], graphsets[k]);
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
            graphsets[place_a] = new graph_set<ewT>(graphsets_a, graphsets_b);
            deleted[place_b] = true;
        }
        std::vector<graph_set<ewT> *> new_graphsets;
        for (int i = 0; i < (int)graphsets.size(); i++) {
            if (!deleted[i])
                new_graphsets.push_back(graphsets[i]);
        }
        return new_graphsets;
    }

    static void simulation(std::vector<graph_set<ewT> *> graphsets) {
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
        std::cout << "Total Work: " << total_work 
                  << ", Avg Work: " << avg_work 
                  << ", Max Work: " << max_work 
                  << ", Min Work: " << min_work
                  << ", Ratio: " << (double)max_work / avg_work 
                  << std::endl;
        uint64_t total_comm = 0, max_comm, min_comm;
        double avg_comm;
        std::vector<uint64_t> comms;
        for (auto graphset: graphsets) {
            bitmap *ret1, *ret2, *ret3;
            ret1 = graphset -> manage_map -> NOT();
            ret2 = graphset -> recv_map -> AND(ret1);
            ret3 = graphset -> send_map -> AND(ret1);
            total_comm += ret2 -> get_size() + ret3 -> get_size();
            comms.push_back(ret2 -> get_size() + ret3 -> get_size());
            delete ret1;
            delete ret2;
            delete ret3;
        }
        sort(comms.begin(), comms.end());
        avg_comm = (double)total_comm / total_block;
        min_comm = comms[0];
        max_comm = comms[total_block - 1];
        std::cout << "Total Comm: " << total_comm 
                  << ", Avg Comm: " << avg_comm
                  << ", Max Comm: " << max_comm 
                  << ", Min Comm: " << min_comm
                  << std::endl;
    }

    void print(bool detail) {
        VLOG(1) << "GraphSet: ";
        for (auto graph: graphs)
            graph -> print(detail);
    }

    static void save(std::vector<graph_set<ewT> *> graphsets, double total_resource) {
        uint32_t total_graph = 0;
        for (auto graphset : graphsets) {
            total_graph += graphset -> graphs.size();
        }
        std::set<uint32_t> cutset;
        for (auto graphset : graphsets) {
            for (auto graph : graphset -> graphs) {
                cutset.insert(graph -> from_source);
                cutset.insert(graph -> from_dest);
                cutset.insert(graph -> to_source);
                cutset.insert(graph -> to_dest);
            }
        }
        std::vector<uint32_t> cuts;
        for (auto cut : cutset) {
            cuts.push_back(cut);
        }
        VLOG(1) << "all cuts: " << log_array<uint32_t>(cuts.data(), cuts.size()).str();
        fs::path root_dir = FLAGS_graph_root_dir;
        if (fs::exists(root_dir)) {
            fs::remove_all(root_dir);
        }
        fs::create_directory(root_dir);
        int *instances = new int[cuts.size() * 2 - 1]();
        instances[0] = total_graph;
        for (int i = 0; i < (int)cuts.size() - 1; i++) {
            for (int j = 0; j < (int)graphsets.size(); j++) {
                bool contain_object_in = false, contain_object_out = false;
                for (auto graph : graphsets[j] -> graphs) {
                    if (cuts[i] >= graph -> from_source && cuts[i] < graph -> to_source) {
                        contain_object_in = true;
                    }
                    if (cuts[i] >= graph -> from_dest && cuts[i] < graph -> to_dest) {
                        contain_object_out = true;
                    }
                }
                if (contain_object_in) {
                    instances[i + 1]++;
                }
                if (contain_object_out) {
                    instances[i + cuts.size()]++;
                }
            }
        }
        for (int i = 0; i < (int)graphsets.size(); i++) {
            VLOG(1) << "graphset " << std::to_string(i) 
                << " resource " << total_resource * graphsets[i] -> edges / FLAGS_edges;
            fs::create_directory(root_dir / std::to_string(i));
            graph_set<ewT> *graphset = graphsets[i];
            json graphs_meta;
            graphs_meta["total_v"] = graphset -> meta.total_v;
            graphs_meta["total_e"] = graphset -> meta.total_e;
            graphs_meta["graphs"] = std::vector<json>();
            for (auto graph : graphset -> graphs) {
                json graph_meta, comm_meta;
                graph_meta["from_source"] = graph -> from_source;
                graph_meta["to_source"] = graph -> to_source;
                graph_meta["from_dest"] = graph -> from_dest;
                graph_meta["to_dest"] = graph -> to_dest;
                graph_meta["edges"] = graph -> edges;
                comm_meta["comm_type"] = CAAS_COMM_MODE::PROXY;
                comm_meta["recv"] = std::vector<json>();
                for (int j = 0; j < (int)cuts.size() - 1; j++) {
                    if (cuts[j] >= graph -> from_source && cuts[j] < graph -> to_source) {
                        json recv_meta;
                        recv_meta["object_id"] = j + 1;
                        recv_meta["start"] = cuts[j];
                        recv_meta["end"]= cuts[j + 1];
                        recv_meta["instances"] = instances[(int)recv_meta["object_id"]];
                        recv_meta["members"] = cuts.size() - 1;
                        if (cuts[j] >= graph -> from_dest && cuts[j] < graph -> to_dest) {
                            recv_meta["root"] = true;
                        } else {
                            recv_meta["root"] = false;
                        }
                        comm_meta["recv"].push_back(recv_meta);
                    }
                }
                if (comm_meta["recv"].size() == 0) {
                    graph -> print(false);
                    LOG(FATAL) << "no in segment";
                } else if (comm_meta["recv"].size() >= 2) {
                    graph -> print(false);
                    LOG(FATAL) << "too many in segments";
                }
                comm_meta["send"] = std::vector<json>();
                for (int j = 0; j < (int)cuts.size() - 1; j++) {
                    if (cuts[j] >= graph -> from_dest && cuts[j] < graph -> to_dest) {
                        json send_meta;
                        send_meta["object_id"] = j + cuts.size();
                        send_meta["start"] = cuts[j];
                        send_meta["end"] = cuts[j + 1];
                        send_meta["instances"] = instances[(int)send_meta["object_id"]];
                        send_meta["members"] = cuts.size() - 1;
                        if (cuts[j] >= graph -> from_source && cuts[j] < graph -> to_source) {
                            send_meta["root"] = true;
                        } else {
                            send_meta["root"] = false;
                        }
                        comm_meta["send"].push_back(send_meta);
                    }
                }
                if (comm_meta["send"].size() == 0) {
                    graph -> print(false);
                    LOG(FATAL) << "no out segment";
                } else if (comm_meta["send"].size() == 2) {
                    graph -> print(false);
                    LOG(FATAL) << "too many out segments";
                }
                comm_meta["vote"]["object_id"] = 0;
                comm_meta["vote"]["instances"] = instances[(int)comm_meta["vote"]["object_id"]];
                comm_meta["vote"]["members"] = total_graph;
                graph_meta["comm"] = comm_meta; 
                graphs_meta["graphs"].push_back(graph_meta);
            }
            std::ofstream graphs_meta_file(root_dir / std::to_string(i) / "graphs.meta");
            graphs_meta_file << std::string(graphs_meta.dump());
            graphs_meta_file.close();
            for (int j = 0; j < (int)graphset -> graphs.size(); j++) {
                graph<ewT> *graph = graphset -> graphs[j];
                save_csr_util(
                    root_dir / std::to_string(i) / std::string("graph" + std::to_string(j) + ".csr.in"),
                    root_dir / std::to_string(i) / std::string("graph" + std::to_string(j) + ".csr.out"),
                    graph -> in_offset, graph -> in_source, graph -> in_weight, graph -> in_degree,
                    graph -> out_offset, graph -> out_dest, graph -> out_weight, graph -> out_degree,
                    graph -> weighted, graph -> to_source - graph -> from_source, graph -> to_dest - graph -> from_dest, 
                    graph -> edges
                );
            }
        }
    }

};

#endif
