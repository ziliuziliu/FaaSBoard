#ifndef _PARTITION_H
#define _PARTITION_H

#include "util/print.h"

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

    void print() {
        std::cout << "Total Blocks: " << blocks.size() << std::endl;
        for (auto block: blocks) {
            std::cout << "Block1: ";
            print_vector<uint32_t>(std::vector<uint32_t>{
                block.from_source, block.to_source, block.from_dest, block.to_dest
            });
            std::cout << ", Size: " << block.edges << std::endl;
            
        }
    }

};

#endif
