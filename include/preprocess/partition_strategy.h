#ifndef _PARTITION_STRATEGY_H
#define _PARTITION_STRATEGY_H

#include "util/log.h"
#include "raw_graph.h"
#include "graph_set.h"
#include "graph.h"

#include <vector>
#include <cassert>
#include <iostream>
#include <cmath>
#include <unordered_map>
#include <functional>
#include <algorithm>

template <class ewT>
class partition_strategy {
public:
    partition_strategy(raw_graph<ewT>& graph, int total_block) : graph(graph), total_block(total_block) {}

    void execute_partition(const std::string& strategy) {
        static const std::unordered_map<std::string, std::function<void()>> strategy_map = {
            {"row", [this]() { handle_row(); }},
            {"column", [this]() { handle_column(); }},
            {"mondriaan_row_column", [this]() { handle_mondriaan_row_column(); }},
            {"mondriaan_column_row", [this]() { handle_mondriaan_column_row(); }},
            {"checkerboard", [this]() { handle_checkerboard(); }},
            {"cycle", [this]() { handle_cycle(); }},
            {"stagger", [this]() { handle_stagger(); }}
        };

        auto it = strategy_map.find(strategy);
        if (it != strategy_map.end()) {
            it->second();
        } else {
            throw std::invalid_argument("Unknown partitioning strategy: " + strategy);
        }
    }

private:
    raw_graph<ewT>& graph;
    int total_block;
    partition_result result;
    std::vector<graph_set<ewT>*> graphsets;

    std::set<uint32_t> extract_cuts(const partition_result& result) {
        std::set<uint32_t> cutset;
        for (auto block : result.blocks) {
            cutset.insert(block.from_source);
            cutset.insert(block.to_source);
            cutset.insert(block.from_dest);
            cutset.insert(block.to_dest);
        }
        return cutset;
    }

    std::vector<uint32_t> convert_cutset_to_vector(const std::set<uint32_t>& cutset) {
        return std::vector<uint32_t>(cutset.begin(), cutset.end());
    }

    // targeted at 4 partition strategies (row,column,mondriaan_row_column,mondiraan_column_row)
    void handle_axis_aligned_partition(std::function<partition_result()> partition_func) {
        result = partition_func();
        result.print();
        std::set<uint32_t> cutset = extract_cuts(result);
        std::vector<uint32_t> cuts = convert_cutset_to_vector(cutset);
        int cut = cuts.size() - 1;
        partition_result processed_result = graph.generate_checkerboard_partition_from_cuts(cut, cuts);
        graphsets = graph.partition(processed_result, false);
        std::vector<graph_set<ewT>*> new_graphsets = graph_set<ewT>::pack_graph(graphsets, result);
        handle_save(true, new_graphsets);
    }

    // Actually, when this function is called, each graphset contains only one graph at this point.
    std::vector<graph_set<ewT>*> Normalize_graphsets() {
        auto compare_graphsets = [](graph_set<ewT>* a, graph_set<ewT>* b) {
            uint32_t flag1_a = a->graphs[0]->from_source;
            uint32_t flag2_a = a->graphs[0]->from_dest;
            uint32_t flag1_b = b->graphs[0]->from_source;
            uint32_t flag2_b = b->graphs[0]->from_dest;
    
            if (flag1_a != flag1_b) {
                return flag1_a < flag1_b;
            }
            return flag2_a < flag2_b;
        };
    
        std::sort(graphsets.begin(), graphsets.end(), compare_graphsets);
        return graphsets;
    }

    void handle_save(bool clean, const std::vector<graph_set<ewT>*>& new_graphsets = {}) {
        graph_set<ewT>::save(clean, new_graphsets.empty() ? graphsets : new_graphsets);
    }

    void handle_row() {
        handle_axis_aligned_partition([this]() { return graph.row_partition(total_block); });
    }

    void handle_column() {
        handle_axis_aligned_partition([this]() { return graph.column_partition(total_block); });
    }

    void handle_mondriaan_row_column() {
        handle_axis_aligned_partition([this]() { return graph.mondriaan_partition_row_column(total_block); });
    }

    void handle_mondriaan_column_row() {
        handle_axis_aligned_partition([this]() { return graph.mondriaan_partition_column_row(total_block); });
    }

    void handle_checkerboard() {
        int cut;
        if (int(std::sqrt(total_block)) * int(std::sqrt(total_block)) == total_block) {
            cut = std::sqrt(total_block);
        } else {
            cut = std::ceil(std::sqrt(total_block));
        }
        for (;;) {
            VLOG(1) << "cut: " << cut;
            result = graph.checkerboard_partition(cut);
            result.print();
            VLOG(1) << "unbalance ratio: " << result.get_unbalance_ratio();;
            if ((int)result.count_non_empty() >= total_block) {
                break;
            }
            VLOG(1) << "not enough blocks, increase cut and try again";
            cut++;
        }
        VLOG(1) << "begin partitioning";
        graphsets = graph.partition(result, true);
        for (auto graphset : graphsets) {
            graphset->print(false);
        }
        while (true) {
            try {
                double balance_ratio;
                std::string save;
                VLOG(1) << "balance ratio?";
                std::cin >> balance_ratio;
                std::vector<graph_set<ewT>*> new_graphsets = graph_set<ewT>::binpack(graphsets, total_block, balance_ratio);
                for (auto graphset : new_graphsets) {
                    graphset->print(false);
                }
                graph_set<ewT>::simulation(new_graphsets);
                VLOG(1) << "save? (Y or N)";
                std::cin >> save;
                if (save == "Y") {
                    handle_save(true, new_graphsets);
                    break;
                }
            } catch (const std::runtime_error& e) {
                std::cout << e.what() << std::endl;
            }
        }
    }

    void handle_cycle() {
        result = graph.naive_checkerboard_partition(total_block);
        result.print();
        VLOG(1) << "unbalance ratio: " << result.get_unbalance_ratio();
        VLOG(1) << "begin partitioning";
        graphsets = graph.partition(result, true);
        Normalize_graphsets();
        for (auto graphset : graphsets) {
            graphset->print(false);
        }
        VLOG(1) << "cycle placing";
        graphsets = graph_set<ewT>::cycle(graphsets, total_block);
        handle_save(true);
    }

    void handle_stagger() {
        result = graph.naive_checkerboard_partition(total_block);
        result.print();
        VLOG(1) << "unbalance ratio: " << result.get_unbalance_ratio();
        VLOG(1) << "begin partitioning";
        graphsets = graph.partition(result, true);
        Normalize_graphsets();
        for (auto graphset : graphsets) {
            graphset->print(false);
        }
        VLOG(1) << "stagger placing";
        graphsets = graph_set<ewT>::stagger(graphsets, total_block);
        handle_save(true);
    }
};

#endif