#ifndef _GRAPH_H
#define _GRAPH_H

#include "partition.h"
#include "util/utils.h"
#include "util/bitmap.h"
#include "util/print.h"

#include <algorithm>
#include <cstdint>
#include <tuple>

struct graph_meta {

    uint32_t total_v, total_e;
    
    graph_meta() {}

    graph_meta(uint32_t total_v, uint32_t total_e):total_v(total_v), total_e(total_e) {}

};

template <class T>
class graph {

public:

    graph_meta meta;
    bool weighted;
    uint32_t from_source, to_source, from_dest, to_dest, edges;
    uint32_t *in_offset, *in_source, *out_offset, *out_dest;
    T *in_weight, *out_weight;

    graph() {}

    graph(partition_block block, graph_meta meta):from_source(block.from_source), to_source(block.to_source),
                                 from_dest(block.from_dest), to_dest(block.to_dest),
                                 edges(block.edges), meta(meta) {
        weighted = !std::is_void<T>::value;
        out_offset = new uint32_t[to_source - from_source + 1]();
        out_dest = new uint32_t[block.edges]();
        in_offset = new uint32_t[to_dest - from_dest + 1]();
        in_source = new uint32_t[block.edges]();
    }

    static bool row_order(graph<T> *a, graph<T> *b) {
        if (a -> from_source != b -> from_source && a -> to_source != b -> to_source)
            return a -> from_source < b -> from_source && a -> to_source < b -> to_source;
        if (a -> from_source == b -> from_source && a -> to_source == b -> to_source &&
            a -> from_dest != b -> from_dest && a -> to_dest != b -> to_dest)
            return a -> from_dest < b -> from_dest && a -> to_dest < b -> to_dest;
        return false;
    }

    static bool column_order(graph<T> *a, graph<T> *b) {
        if (a -> from_dest != b -> from_dest && a -> to_dest != b -> to_dest)
            return a -> from_dest < b -> from_dest && a -> to_dest < b -> to_dest;
        if (a -> from_dest == b -> from_dest && a -> to_dest == b -> to_dest &&
            a -> from_source != b -> from_source && a -> to_source != b -> to_source)
            return a -> from_source < b -> from_source && a -> to_source < b -> to_source;
        return false;
    }

    bool edge_is_in(uint32_t u, uint32_t v) {
        return u >= from_source && u < to_source && v >= from_dest && v < to_dest;
    }

    void begin_add_edge(uint32_t v, int direction) {
        if (direction == OUTGOING && v != from_source)
            out_offset[v - from_source] = out_offset[v - 1 - from_source];
        else if (direction == INCOMING && v != from_dest)
            in_offset[v - from_dest] = in_offset[v - 1 - from_dest];
    }    

    void add_edge(uint32_t u, uint32_t v, int direction) {
        if (direction == OUTGOING)
            out_dest[out_offset[u - from_source]++] = v;
        else if (direction == INCOMING)
            in_source[in_offset[v - from_dest]++] = u;
    }

    void add_edge(uint32_t u, uint32_t v, T w, int direction) {
        if (direction == OUTGOING) {
            out_dest[out_offset[u - from_source]] = v;
            out_weight[out_offset[u - from_source]++] = w;
        }
        else if (direction == INCOMING) {
            in_source[in_offset[v - from_dest]] = u;
            in_weight[in_offset[v - from_dest]++] = w;
        }
    }

    void end_add_edge(int direction) {
        if (direction == OUTGOING) {
            for (uint32_t i = to_source; i > from_source; i--)
                out_offset[i - from_source] = out_offset[i - 1 - from_source];
            out_offset[0] = 0;
        } else if (direction == INCOMING) {
            for (uint32_t i = to_dest; i > from_dest; i--)
                in_offset[i - from_dest] = in_offset[i - 1 - from_dest];
            in_offset[0] = 0;
        }
    }

    std::tuple<uint32_t *, T *, uint32_t> in_edge(uint32_t v) {
        v -= from_dest;
        T *weight = weighted ? in_weight + in_offset[v] : nullptr;
        return std::make_tuple(in_source + in_offset[v], weight, in_offset[v + 1] - in_offset[v]);
    }

    std::tuple<uint32_t *, T *, uint32_t> out_edge(uint32_t v) {
        v -= from_source;
        T *weight = weighted ? out_weight + out_offset[v] : nullptr;
        return std::make_tuple(out_dest + out_offset[v], weight, out_offset[v + 1] - out_offset[v]);
    }

    std::vector<uint32_t> get_boundary() {
        return std::vector<uint32_t>{from_source, to_source, from_dest, to_dest};
    }

    void print() {
        std::cout << "Block: ";
        print_vector<uint32_t>(std::vector<uint32_t>{
            from_source, to_source, from_dest, to_dest
        });
        std::cout << std::endl;
        std::cout << "In Offset: ";
        print_array<uint32_t>(in_offset, to_dest - from_dest + 1);
        std::cout << std::endl;
        std::cout << "In Source: ";
        print_array<uint32_t>(in_source, edges);
        std::cout << std::endl;
        std::cout << "Out Offset: ";
        print_array<uint32_t>(out_offset, to_source - from_source + 1);
        std::cout << std::endl;
        std::cout << "Out Dest: ";
        print_array<uint32_t>(out_dest, edges);
        std::cout << std::endl;
    }

};

#endif
