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

};

struct partition_result {

    int row, column;
    std::vector<partition_block> blocks;

    partition_result() {}
    partition_result(int row, int column):row(row),column(column) {}

    void add(
        uint32_t from_source, uint32_t to_source, uint32_t from_dest, uint32_t to_dest,
        uint32_t edges
    ) {
        blocks.push_back(partition_block(
            from_source, to_source, from_dest, to_dest, edges
        ));
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
