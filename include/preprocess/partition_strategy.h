#ifndef _PARTITION_STRATEGY_H
#define _PARTITION_STRATEGY_H

#include "util/log.h"
#include "raw_graph.h"
#include "graph_set.h"

#include <vector>
#include <cassert>
#include <iostream>
#include <cmath>
#include <unordered_map>
#include <functional>

template <class ewT>
class PartitionStrategy {
public:
    PartitionStrategy(raw_graph<ewT>& g, int total_block):g_(g), total_block_(total_block) {}

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

    partition_result get_result() const {
        return result_;
    }

    std::vector<graph_set<ewT>*> get_graphsets() const {
        return graphsets_;
    }

private:
    raw_graph<ewT>& g_;
    int total_block_;
    partition_result result_;
    std::vector<graph_set<ewT>*> graphsets_;

    void handle_row() {
        result_ = g_.row_partition(total_block_);
        result_.print();
        
        std::set<uint32_t> cutset;
        for(auto block : result_.blocks){
            cutset.insert(block.from_source);
            cutset.insert(block.to_source);
            cutset.insert(block.from_dest);
            cutset.insert(block.to_dest);
        }
        std::vector<uint32_t> cuts;
        for (auto cut : cutset) {
            cuts.push_back(cut);
        }
        int cut = (int)cuts.size() - 1;

        VLOG(1) << "all cuts: " << log_array<uint32_t>(cuts.data(), cuts.size()).str()<<" cut = " <<cut;
        VLOG(1) << "start generate_checkerboard_partition_from_cuts:";
        partition_result processed_result = g_.generate_checkerboard_partition_from_cuts(cut, cuts);
        VLOG(1) << "start row_partition";
        graphsets_ = g_.partition(processed_result);
        std::vector<graph_set<ewT> *> new_graphsets = graph_set<ewT>::binpack_for_owners(graphsets_, result_, total_block_);
        double total_resource;
        VLOG(1) << "total resource (cores)?";
        std::cin >> total_resource;
        graph_set<ewT>::save(new_graphsets, total_resource);
    }

    void handle_column() {
        result_ = g_.column_partition(total_block_);
        result_.print();
        std::set<uint32_t> cutset;
        for(auto block : result_.blocks){
            cutset.insert(block.from_source);
            cutset.insert(block.to_source);
            cutset.insert(block.from_dest);
            cutset.insert(block.to_dest);
        }
        std::vector<uint32_t> cuts;
        for (auto cut : cutset) {
            cuts.push_back(cut);
        }
        int cut = (int)cuts.size() - 1;
        partition_result processed_result_ = g_.generate_checkerboard_partition_from_cuts(cut , cuts);
        graphsets_ = g_.partition(processed_result_);
        std::vector<graph_set<ewT> *> new_graphsets = graph_set<ewT>::binpack_for_owners(graphsets_, result_, total_block_);
        double total_resource;
        VLOG(1) << "total resource (cores)?";
        std::cin >> total_resource;
        graph_set<ewT>::save(new_graphsets, total_resource);
    }

    void handle_mondriaan_row_column() {
        result_ = g_.mondriaan_partition_row_column(total_block_);
        result_.print();
        std::set<uint32_t> cutset;
        for(auto block : result_.blocks){
            cutset.insert(block.from_source);
            cutset.insert(block.to_source);
            cutset.insert(block.from_dest);
            cutset.insert(block.to_dest);
        }
        std::vector<uint32_t> cuts;
        for (auto cut : cutset) {
            cuts.push_back(cut);
        }
        int cut = (int)cuts.size() - 1;
        partition_result processed_result_ = g_.generate_checkerboard_partition_from_cuts(cut , cuts);
        graphsets_ = g_.partition(processed_result_);
        std::vector<graph_set<ewT> *> new_graphsets = graph_set<ewT>::binpack_for_owners(graphsets_, result_, total_block_);
        double total_resource;
        VLOG(1) << "total resource (cores)?";
        std::cin >> total_resource;
        graph_set<ewT>::save(new_graphsets, total_resource);
    }

    void handle_mondriaan_column_row() {
        result_ = g_.mondriaan_partition_column_row(total_block_);
        result_.print();
        std::set<uint32_t> cutset;
        for(auto block : result_.blocks){
            cutset.insert(block.from_source);
            cutset.insert(block.to_source);
            cutset.insert(block.from_dest);
            cutset.insert(block.to_dest);
        }
        std::vector<uint32_t> cuts;
        for (auto cut : cutset) {
            cuts.push_back(cut);
        }
        int cut = (int)cuts.size() - 1;
        partition_result processed_result_ = g_.generate_checkerboard_partition_from_cuts(cut , cuts);
        graphsets_ = g_.partition(processed_result_);
        std::vector<graph_set<ewT> *> new_graphsets = graph_set<ewT>::binpack_for_owners(graphsets_, result_, total_block_);
        double total_resource;
        VLOG(1) << "total resource (cores)?";
        std::cin >> total_resource;
        graph_set<ewT>::save(new_graphsets, total_resource);
    }

    void handle_checkerboard() {
        int cut = sqrt((double)total_block_) + 1;
        result_ = g_.checkerboard_partition(cut);
        result_.print();
        VLOG(1) << "unbalance ratio: " << result_.get_unbalance_ratio();
        VLOG(1) << "begin partitioning";
        graphsets_ = g_.partition(result_);
        for (auto graphset : graphsets_) {
            graphset->print(false);
        }

        /*
        int cut = 3;
        std::vector<uint32_t> cuts;
        cuts.push_back(0);
        cuts.push_back(483967);
        cuts.push_back(1533312);
        cuts.push_back(4847571);
        partition_result processed_result_ = g_.generate_checkerboard_partition_from_cuts(cut , cuts);
        graphsets_ = g_.partition(processed_result_);
        VLOG(1) << "all cuts: " << log_array<uint32_t>(cuts.data(), cuts.size()).str()<<" cut = " <<cut;
        */

        while (true) {
            try {
                double balance_ratio;
                std::string save;
                VLOG(1) << "balance ratio?";
                std::cin >> balance_ratio;
                std::vector<graph_set<ewT> *> new_graphsets = graph_set<ewT>::binpack(graphsets_, total_block_, balance_ratio);
                for (auto graphset : new_graphsets) {
                    graphset->print(false);
                }
                graph_set<ewT>::simulation(new_graphsets);
                VLOG(1) << "save? (Y or N)";
                std::cin >> save;
                if (save == "Y") {
                    double total_resource;
                    VLOG(1) << "total resource (cores)?";
                    std::cin >> total_resource;
                    graph_set<ewT>::save(new_graphsets, total_resource);
                    int j=0;
                    for(auto graphset : new_graphsets){
                        j++;
                        VLOG(1) << "num = "<<j; 
                        graphset -> print(false);
                    }
                    break;
                }
            } catch (const std::runtime_error& e) {
                std::cout << e.what() << std::endl;
            }
        }

    }

    void handle_cycle() {
        int cut = total_block_;
        VLOG(1) << "cyclic cut";
        result_ = g_.naive_checkerboard_partition(cut);
        result_.print();
        VLOG(1) << "unbalance ratio: " << result_.get_unbalance_ratio();
        VLOG(1) << "begin partitioning";
        graphsets_ = g_.partition(result_);
        VLOG(1) << "cycle placing";
        graphsets_ = graph_set<ewT>::cycle(graphsets_, total_block_);
        double total_resource;
        VLOG(1) << "total resource (cores)?";
        std::cin >> total_resource;
        VLOG(1) << "start saving";
        graph_set<ewT>::save(graphsets_, total_resource);
    }

    void handle_stagger() {
        int cut = total_block_;
        VLOG(1) << "stagger cut";
        result_ = g_.naive_checkerboard_partition(cut);
        result_.print();
        VLOG(1) << "unbalance ratio: " << result_.get_unbalance_ratio();
        VLOG(1) << "begin partitioning";
        graphsets_ = g_.partition(result_);
        VLOG(1) << "stagger placing";
        graphsets_ = graph_set<ewT>::stagger(graphsets_, total_block_);
        double total_resource;
        VLOG(1) << "total resource (cores)?";
        std::cin >> total_resource;
        VLOG(1) << "start saving";
        graph_set<ewT>::save(graphsets_, total_resource);
    }
};

#endif