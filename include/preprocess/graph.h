#ifndef _GRAPH_H
#define _GRAPH_H

#include "partition.h"
#include "util/io.h"
#include "util/bitmap.h"
#include "util/types.h"

#include <algorithm>
#include <cstdint>
#include <tuple>
#include <cstdlib>

struct graph_meta {

    uint32_t total_v, total_e;
    
    graph_meta() {}

    graph_meta(uint32_t total_v, uint32_t total_e):total_v(total_v), total_e(total_e) {}

};

template <class ewT>
class graph {

public:

    uint32_t id;
    graph_meta meta;
    bool weighted;
    uint32_t from_source, to_source, from_dest, to_dest, edges;
    uint32_t *in_offset, *in_source, *out_offset, *out_dest;
    ewT *in_weight, *out_weight;

    graph() {}

    graph(partition_block block, graph_meta meta): meta(meta),
                                 from_source(block.from_source), to_source(block.to_source),
                                 from_dest(block.from_dest), to_dest(block.to_dest),
                                 edges(block.edges) {
        id = rand();
        weighted = !std::is_same<ewT, void *>::value;
        out_offset = new uint32_t[to_source - from_source + 1]();
        out_dest = new uint32_t[block.edges]();
        in_offset = new uint32_t[to_dest - from_dest + 1]();
        in_source = new uint32_t[block.edges]();
        if (weighted) {
            in_weight = new ewT[block.edges]();
            out_weight = new ewT[block.edges]();
        }
    }

    static bool row_order(graph<ewT> *a, graph<ewT> *b) {
        if (a -> from_source != b -> from_source && a -> to_source != b -> to_source)
            return a -> from_source < b -> from_source && a -> to_source < b -> to_source;
        if (a -> from_source == b -> from_source && a -> to_source == b -> to_source &&
            a -> from_dest != b -> from_dest && a -> to_dest != b -> to_dest)
            return a -> from_dest < b -> from_dest && a -> to_dest < b -> to_dest;
        return false;
    }

    static bool column_order(graph<ewT> *a, graph<ewT> *b) {
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

    void add_edge(uint32_t u, uint32_t v, ewT w, int direction) {
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

    std::tuple<uint32_t *, ewT *, uint32_t> in_edge(uint32_t v) {
        v -= from_dest;
        ewT *weight = weighted ? in_weight + in_offset[v] : nullptr;
        return std::make_tuple(in_source + in_offset[v], weight, in_offset[v + 1] - in_offset[v]);
    }

    std::tuple<uint32_t *, ewT *, uint32_t> out_edge(uint32_t v) {
        v -= from_source;
        ewT *weight = weighted ? out_weight + out_offset[v] : nullptr;
        return std::make_tuple(out_dest + out_offset[v], weight, out_offset[v + 1] - out_offset[v]);
    }

    std::vector<uint32_t> get_boundary() {
        return std::vector<uint32_t>{from_source, to_source, from_dest, to_dest};
    }

    void check() {
        for (uint32_t i = 0; i < to_dest - from_dest; i++) {
            CHECK(in_offset[i] <= in_offset[i + 1]) << "in_offset[" << i << "] > in_offset[" << i + 1 << "]";
        }
        for (uint32_t i = 0; i < to_source - from_source; i++) {
            CHECK(out_offset[i] <= out_offset[i + 1]) << "out_offset[" << i << "] > out_offset[" << i + 1 << "]";
        }
    }

    void print(bool detail) {
        VLOG(1) << "ID: " << id << ", Block: { " 
                            << from_source << ", " 
                            << to_source << ", " 
                            << from_dest << ", " 
                            << to_dest << " }, Size: "
                            << edges;
        if (!detail) {
            return;
        }
        VLOG(2) << "In Offset: " << log_array<uint32_t>(in_offset, to_dest - from_dest + 1).str();
        VLOG(2) << "In Source: " << log_array<uint32_t>(in_source, edges).str();
        VLOG(2) << "Out Offset: " << log_array<uint32_t>(out_offset, to_source - from_source + 1).str();
        VLOG(2) << "Out Dest: " << log_array<uint32_t>(out_dest, edges).str();
    }

};

#endif
