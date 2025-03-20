#ifndef _PARTITION_H
#define _PARTITION_H

#include "util/log.h"

#include <vector>
#include <cassert>
#include <iostream>

struct partition_block {

    uint32_t from_source, to_source, from_dest, to_dest, edges;

    partition_block() {}
    
    partition_block(
        uint32_t from_source, uint32_t to_source,
        uint32_t from_dest, uint32_t to_dest, 
        uint32_t edges
    ):from_source(from_source), to_source(to_source),
      from_dest(from_dest), to_dest(to_dest), 
      edges(edges) {}


    bool root() {
        return from_source == from_dest && to_source == to_dest;
    }
};

struct partition_result {

    std::vector<partition_block> blocks;

    partition_result() {}

    void add(
        uint32_t from_source, uint32_t to_source, uint32_t from_dest, uint32_t to_dest,
        uint32_t edges
    ) {
        blocks.push_back(partition_block(from_source, to_source, from_dest, to_dest, edges));
    }

    double get_unbalance_ratio() {
        double avg_workload = 0, max_workload = 0;
        for (auto block : blocks) {
            max_workload = std::max(max_workload, (double)block.edges);
            avg_workload += block.edges;
        }
        avg_workload /= blocks.size();
        return max_workload / avg_workload;
    }

    int count_non_empty() {
        int cnt = 0;
        for (auto block : blocks) {
            if (block.edges > 0) {
                cnt++;
            }
        }
        return cnt;
    }

    void print() {
        VLOG(1) << "Total Blocks: " << blocks.size();
        for (auto block: blocks) {
            VLOG(1) << "Block: { " 
                      << block.from_source << ", " 
                      << block.to_source << ", " 
                      << block.from_dest << ", " 
                      << block.to_dest << " }, Size: " 
                      << block.edges;
        }
    }

};

#endif
