#ifndef _RAW_GRAPH_H
#define _RAW_GRAPH_H

#include "graph.h"
#include "graph_set.h"
#include "partition.h"
#include "util/io.h"
#include "util/timer.h"
#include "util/types.h"

#include <algorithm>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <cmath>
#include <utility>
#include <cassert>
#include <omp.h>
#include <errno.h>
#include <metis.h>
#include <limits>

template <class ewT>
class raw_graph {

public:

    graph_meta meta;
    bool weighted;
    uint64_t *in_offset, *out_offset;
    uint32_t *in_source, *out_dest, *in_degree, *out_degree;
    ewT *in_weight, *out_weight;

    raw_graph() {}

    raw_graph(uint32_t total_v, uint64_t total_e) {
        meta = graph_meta(total_v, total_e);
        weighted = !std::is_same<ewT, void *>::value;
        in_offset = new uint64_t[meta.total_v + 1]();
        in_source = new uint32_t[meta.total_e]();
        in_degree = new uint32_t[meta.total_v]();
        out_offset = new uint64_t[meta.total_v + 1]();
        out_dest = new uint32_t[meta.total_e]();
        out_degree = new uint32_t[meta.total_v]();
        if (weighted) {
            in_weight = new ewT[meta.total_e]();
            out_weight = new ewT[meta.total_e]();
        }
    }

    void read_txt(std::string path, bool undirected, bool txt_with_weight) {
        read_txt_util<ewT>(
            path, undirected, txt_with_weight,
            in_offset, in_source, in_weight, in_degree,
            out_offset, out_dest, out_weight, out_degree, 
            weighted, meta.total_v, meta.total_e
        );
    }

    void read_csr(std::string in_path, std::string out_path) {
        read_csr_util<ewT, uint64_t>(
            in_path, out_path, 
            in_offset, in_source, in_weight, in_degree,
            out_offset, out_dest, out_weight, out_degree, 
            weighted, false, false, true, meta.total_v, meta.total_v, meta.total_e
        );
    }

    void save_csr(std::string in_path, std::string out_path) {
        save_csr_util<ewT, uint64_t>(
            in_path, out_path, 
            in_offset, in_source, in_weight, in_degree,
            out_offset, out_dest, out_weight, out_degree, 
            weighted, meta.total_v, meta.total_v, meta.total_e
        );
    }

    partition_result row_partition(int total_block) {
        timer t;
        t.tick("partition time");
        partition_result result;
        uint64_t current_edges = 0, previous_from_source = 0;
        for (uint32_t i = 0; i < meta.total_v; i++) {
            if (i % 64 == 0 && current_edges * total_block >= meta.total_e) {
                result.add(previous_from_source, i, 0, meta.total_v, current_edges);
                previous_from_source = i;
                current_edges = 0;
            }
            current_edges += out_offset[i + 1] - out_offset[i];
        }
        result.add(previous_from_source, meta.total_v, 0, meta.total_v, current_edges);
        t.from_tick();
        return result;
    }

    partition_result column_partition(int total_block) {
        timer t;
        t.tick("partition time");
        partition_result result;
        uint64_t current_edges = 0, previous_from_dest = 0;
        for (uint32_t i = 0; i < meta.total_v; i++) {
            if (i % 64 == 0 && current_edges * total_block >= meta.total_e) {
                result.add(0, meta.total_v, previous_from_dest, i, current_edges);
                previous_from_dest = i;
                current_edges = 0;
            }
            current_edges += in_offset[i + 1] - in_offset[i];
        }
        result.add(0, meta.total_v, previous_from_dest, meta.total_v, current_edges);
        t.from_tick();
        return result;
    }

    partition_result mondriaan_partition_row_column(int total_block) {
        timer t;
        t.tick("partition time");
        std::pair<int,int> rank;
        rank = factorizeInt(total_block);
        int row_rank = rank.first, col_rank = rank.second;
        VLOG(1) << "row_rank = " << row_rank << "; col_rank = " << col_rank;
        partition_result row_result = row_partition(row_rank);
        row_result.print();
        partition_result final_result;
        #pragma omp parallel for
        for (int t = 0; t < (int)row_result.blocks.size(); t++) {
            partition_block block = row_result.blocks[t];
            uint32_t accum_edges = 0, previous_from_dest = 0;
            for (uint32_t i = 0; i < meta.total_v; i++) {
                if (i % 64 == 0 && accum_edges * col_rank >= block.edges) {
                    #pragma omp critical
                    {
                        final_result.add(block.from_source, block.to_source, previous_from_dest, i, accum_edges);
                        accum_edges = 0;
                        previous_from_dest = i;
                    }
                }
                uint32_t current_edges = 0;
                for (uint64_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
                    uint32_t source = in_source[j];
                    if (source >= block.from_source && source < block.to_source) {
                        current_edges++;
                    }
                }
                accum_edges += current_edges;
            }
            #pragma omp critical
            {
                final_result.add(block.from_source, block.to_source, previous_from_dest, meta.total_v, accum_edges);
            }
        }
        t.from_tick();
        return final_result;
    }

    partition_result mondriaan_partition_column_row(int total_block) {
        timer t;
        t.tick("partition time");
        std::pair<int,int> rank;
        rank = factorizeInt(total_block);
        int row_rank = rank.first, col_rank = rank.second;
        VLOG(1) << "row_rank = " << row_rank << "; col_rank = " << col_rank;
        partition_result column_result = row_partition(row_rank);
        partition_result final_result;
        #pragma omp parallel for
        for (int t = 0; t < (int)column_result.blocks.size(); t++) {
            partition_block block = column_result.blocks[t];
            uint32_t accum_edges = 0, previous_from_source = 0;
            for (uint32_t i = 0; i < meta.total_v; i++) {
                if (i % 64 == 0 && accum_edges * row_rank >= block.edges) {
                    #pragma omp critical
                    {
                        final_result.add(previous_from_source, i, block.from_dest, block.to_dest, accum_edges);
                        accum_edges = 0;
                        previous_from_source = i;
                    }
                }
                uint32_t current_edges = 0;
                for (uint64_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                    uint32_t dest = out_dest[j];
                    if (dest >= block.from_dest && dest < block.to_dest) {
                        current_edges++;
                    }
                }
                accum_edges += current_edges;
            }
            #pragma omp critical
            {
                final_result.add(previous_from_source, meta.total_v, block.from_dest, block.to_dest, accum_edges);
            }
        }
        t.from_tick();
        return final_result;
    }

    // reorder by HASH partition and rebuild CSR.
    void reorder_vertices_by_hash(int total_block) {
        if (total_block <= 0) {
            LOG(FATAL) << "reorder_vertices_by_hash: total_block must be > 0, got " << total_block;
        }
        if (meta.total_v == 0) return;

        // hash_bucket: partition(i) = i % total_block
        std::vector<uint32_t> cnt(total_block, 0); // num_vertices of every partition
        for (uint32_t i = 0; i < meta.total_v; i++) {
            cnt[i % total_block]++;
        }

        // prefix[i] is the start_pos of re-ordered partition i, prefix[total_block] is equal to meta.total_v
        std::vector<uint32_t> prefix(total_block + 1, 0);
        for (int b = 0; b < total_block; b++) {
            prefix[b + 1] = prefix[b] + cnt[b];
        }

        std::vector<uint32_t> write_pos = prefix; // write_pointer for every bucket
        std::vector<uint32_t> new2old(meta.total_v, 0);
        std::vector<uint32_t> old2new(meta.total_v, 0);

        for (uint32_t i = 0; i < meta.total_v; i++) {
            uint32_t bucket = i % total_block;
            uint32_t new_pos = write_pos[bucket]++;
            new2old[new_pos] = i;
            old2new[i] = new_pos;
        }

        // Build OUT CSR for re-ordered graph
        uint64_t* new_out_offset = new uint64_t[meta.total_v + 1]();
        uint32_t* new_out_degree = new uint32_t[meta.total_v]();
        uint32_t* new_out_dest = new uint32_t[meta.total_e]();
        ewT* new_out_weight = nullptr;
        if (weighted) new_out_weight = new ewT[meta.total_e]();

        new_out_offset[0] = 0;
        for (uint32_t i = 0; i < meta.total_v; i++) {
            uint32_t old_i = new2old[i];
            uint64_t deg = out_offset[old_i + 1] - out_offset[old_i];
            new_out_degree[i] = static_cast<uint32_t>(deg);
            new_out_offset[i + 1] = new_out_offset[i] + deg;
        }

        if (new_out_offset[meta.total_v] != meta.total_e) {
            LOG(FATAL) << "reorder_vertices_by_hash: new_out_offset[meta.total_v] != metal.total_e, got "
                    << new_out_offset[meta.total_v] << ", expected " << meta.total_e;
        }

        for (uint32_t i = 0; i < meta.total_v; i++) {
            uint32_t old_i = new2old[i];
            uint64_t old_begin = out_offset[old_i];
            uint64_t old_end = out_offset[old_i + 1];
            uint64_t new_begin = new_out_offset[i];

            uint64_t k = 0;
            for (uint64_t e = old_begin; e < old_end; e++, k++) {
                uint32_t old_j = out_dest[e];
                uint32_t new_j = old2new[old_j];
                new_out_dest[new_begin + k] = new_j;
                if (weighted) {
                    new_out_weight[new_begin + k] = out_weight[e];
                }
            }
        }

        // Build IN CSR for re-ordered graph
        uint64_t* new_in_offset = new uint64_t[meta.total_v + 1]();
        uint32_t* new_in_degree = new uint32_t[meta.total_v]();
        uint32_t* new_in_source = new uint32_t[meta.total_e]();
        ewT* new_in_weight = nullptr;
        if (weighted) new_in_weight = new ewT[meta.total_e]();

        new_in_offset[0] = 0;
        for (uint32_t i = 0; i < meta.total_v; i++) {
            uint32_t old_i = new2old[i];
            uint64_t deg = in_offset[old_i + 1] - in_offset[old_i];
            new_in_degree[i] = static_cast<uint32_t>(deg);
            new_in_offset[i + 1] = new_in_offset[i] + deg;
        }

        if (new_in_offset[meta.total_v] != meta.total_e) {
            LOG(FATAL) << "reorder_vertices_by_hash: new_in_offset[meta.total_v] != meta.total_e, got "
                    << new_in_offset[meta.total_v] << ", expected " << meta.total_e;
        }

        for (uint32_t i = 0; i < meta.total_v; i++) {
            uint32_t old_i = new2old[i];
            uint64_t old_begin = in_offset[old_i];
            uint64_t old_end = in_offset[old_i + 1];
            uint64_t new_begin = new_in_offset[i];

            uint64_t k = 0;
            for (uint64_t e = old_begin; e < old_end; e++, k++) {
                uint32_t old_j = in_source[e];
                uint32_t new_j = old2new[old_j];
                new_in_source[new_begin + k] = new_j;
                if (weighted) {
                    new_in_weight[new_begin + k] = in_weight[e];
                }
            }
        }

        // refresh and GC
        delete[] out_offset;
        delete[] out_degree;
        delete[] out_dest;
        out_offset = new_out_offset;
        out_degree = new_out_degree;
        out_dest   = new_out_dest;
        if (weighted) {
            delete[] out_weight;
            out_weight = new_out_weight;
        }

        delete[] in_offset;
        delete[] in_degree;
        delete[] in_source;
        in_offset = new_in_offset;
        in_degree = new_in_degree;
        in_source = new_in_source;
        if (weighted) {
            delete[] in_weight;
            in_weight = new_in_weight;
        }
    }

    partition_result hash_partition(int total_block) {
        timer t;
        t.tick("partition time");

        // do reordering first
        reorder_vertices_by_hash(total_block);

        // chunking (row-wise, similar to row_partition)
        partition_result result;

        std::vector<uint32_t> cnt(total_block, 0);
        for (uint32_t i = 0; i < meta.total_v; i++) {
            cnt[i % total_block]++;
        }

        uint32_t target_vertices =
            (meta.total_v + total_block - 1) / total_block;

        uint32_t previous_from_source = 0;
        uint64_t current_edges = 0;
        int formed_blocks = 0;

        for (uint32_t i = 0; i < meta.total_v; i++) {
            current_edges += out_offset[i + 1] - out_offset[i];

            bool can_cut =
                (formed_blocks < total_block - 1) && // last_partition
                (i % 64 == 0) &&
                ((i - previous_from_source) >= target_vertices);

            if (can_cut) {
                result.add(previous_from_source, i, 0, meta.total_v, static_cast<uint32_t>(current_edges));
                previous_from_source = i;
                current_edges = 0;
                formed_blocks++;
            }
        }

        result.add(previous_from_source, meta.total_v, 0, meta.total_v, static_cast<uint32_t>(current_edges));

        t.from_tick();
        return result;
    }

    // reorder by METIS partition and rebuild CSR.
    void reorder_vertices_by_metis_parts(const std::vector<uint32_t>& part, int total_block, std::vector<uint32_t>& metis_prefix) {

        timer t;
        t.tick("reorder_by_metis");

        // count vertices per partition
        std::vector<uint32_t> cnt(total_block, 0);
        for (uint32_t u = 0; u < meta.total_v; u++) {
            uint32_t p = part[u];
            if (p >= (uint32_t)total_block) {
                LOG(FATAL) << "invalid METIS part id: " << p;
            }
            cnt[p]++;
        }

        // compute aligned prefix
        std::vector<uint32_t> prefix(total_block + 1, 0);
        for (int p = 0; p < total_block; p++) {
            uint32_t start = prefix[p];
            uint32_t aligned_start =
                (start + 64 - 1) / 64 * 64;
            prefix[p] = aligned_start;
            prefix[p + 1] = aligned_start + cnt[p];
        }

        uint32_t new_total_v = prefix[total_block];
        metis_prefix = prefix;  

        // build mapping
        std::vector<uint32_t> write_pos(prefix.begin(), prefix.end());
        std::vector<uint32_t> new2old(new_total_v, UINT32_MAX);
        std::vector<uint32_t> old2new(meta.total_v, 0);

        for (uint32_t u = 0; u < meta.total_v; u++) {
            uint32_t p = part[u];
            uint32_t new_pos = write_pos[p]++;
            new2old[new_pos] = u;
            old2new[u] = new_pos;
        }

        // rebuild OUT CSR
        uint64_t* new_out_offset = new uint64_t[new_total_v + 1]();
        uint32_t* new_out_degree = new uint32_t[new_total_v]();
        uint32_t* new_out_dest   = new uint32_t[meta.total_e]();

        new_out_offset[0] = 0;
        for (uint32_t i = 0; i < new_total_v; i++) {
            if (new2old[i] == UINT32_MAX) {
                new_out_degree[i] = 0;
                new_out_offset[i + 1] = new_out_offset[i];
            } else {
                uint32_t old_i = new2old[i];
                uint64_t deg = out_offset[old_i + 1] - out_offset[old_i];
                new_out_degree[i] = (uint32_t)deg;
                new_out_offset[i + 1] = new_out_offset[i] + deg;
            }
        }

        if (new_out_offset[new_total_v] != meta.total_e) {
            LOG(FATAL) << "reorder_by_metis: edge count mismatch";
        }

        for (uint32_t i = 0; i < new_total_v; i++) {
            if (new2old[i] == UINT32_MAX) continue;
            uint32_t old_i = new2old[i];
            uint64_t old_b = out_offset[old_i];
            uint64_t old_e = out_offset[old_i + 1];
            uint64_t new_b = new_out_offset[i];

            uint64_t k = 0;
            for (uint64_t e = old_b; e < old_e; e++, k++) {
                uint32_t old_j = out_dest[e];
                new_out_dest[new_b + k] = old2new[old_j];
            }
        }

        // Rebuild IN CSR
        uint64_t* new_in_offset = new uint64_t[new_total_v + 1]();
        uint32_t* new_in_degree = new uint32_t[new_total_v]();
        uint32_t* new_in_source = new uint32_t[meta.total_e]();

        new_in_offset[0] = 0;
        for (uint32_t i = 0; i < new_total_v; i++) {
            if (new2old[i] == UINT32_MAX) {
                new_in_degree[i] = 0;
                new_in_offset[i + 1] = new_in_offset[i];
            } else {
                uint32_t old_i = new2old[i];
                uint64_t deg = in_offset[old_i + 1] - in_offset[old_i];
                new_in_degree[i] = (uint32_t)deg;
                new_in_offset[i + 1] = new_in_offset[i] + deg;
            }
        }

        if (new_in_offset[new_total_v] != meta.total_e) {
            LOG(FATAL) << "reorder_by_metis: in-edge count mismatch";
        }

        for (uint32_t i = 0; i < new_total_v; i++) {
            if (new2old[i] == UINT32_MAX) continue;
            uint32_t old_i = new2old[i];
            uint64_t old_b = in_offset[old_i];
            uint64_t old_e = in_offset[old_i + 1];
            uint64_t new_b = new_in_offset[i];

            uint64_t k = 0;
            for (uint64_t e = old_b; e < old_e; e++, k++) {
                uint32_t old_j = in_source[e];
                new_in_source[new_b + k] = old2new[old_j];
            }
        }

        // replace graph
        delete[] out_offset;
        delete[] out_degree;
        delete[] out_dest;
        out_offset = new_out_offset;
        out_degree = new_out_degree;
        out_dest   = new_out_dest;

        delete[] in_offset;
        delete[] in_degree;
        delete[] in_source;
        in_offset = new_in_offset;
        in_degree = new_in_degree;
        in_source = new_in_source;

        meta.total_v = new_total_v;

        t.from_tick();
    }

    partition_result metis_partition(int total_block) {
        timer t_total;
        t_total.tick("metis_partition_total");

        partition_result result;

        // partitions == 1 -> no need for METIS
        if (total_block == 1) {
            uint32_t e32 = (meta.total_e > UINT32_MAX) ? UINT32_MAX : static_cast<uint32_t>(meta.total_e);
            result.add(0, meta.total_v, 0, meta.total_v, e32);
            VLOG(1) << "metis_partition: total_block==1, skip METIS.";
            t_total.from_tick();
            return result;
        }

        // Basic runtime sanity
        VLOG(1) << "METIS IDXTYPEWIDTH=" << IDXTYPEWIDTH << ", sizeof(idx_t)=" << sizeof(idx_t);

        const idx_t n = static_cast<idx_t>(meta.total_v);
        idx_t nparts  = static_cast<idx_t>(total_block);

        // Build undirected, unweighted CSR for METIS
        timer t_build;
        t_build.tick("metis_build_csr");

        struct Nbr { uint32_t v; };

        // accumulate xadj in uint64_t then convert to idx_t
        std::vector<uint64_t> xadj64(static_cast<size_t>(meta.total_v) + 1, 0);

        {
            std::vector<Nbr> buf;

            for (uint32_t u = 0; u < meta.total_v; u++) {
                buf.clear();

                // reserve may throw for extremely high-degree vertices; safe-guard it.
                uint64_t want = (uint64_t)out_degree[u] + (uint64_t)in_degree[u];
                if (want > 0) {
                    // cap reserve to avoid pathological huge reserve requests
                    uint64_t cap = std::min<uint64_t>(want, 50ull * 1000ull * 1000ull); // 50M
                    buf.reserve((size_t)cap);
                }

                // out-neighbors
                for (uint64_t e = out_offset[u]; e < out_offset[u + 1]; e++) {
                    uint32_t v = out_dest[e];
                    if (v != u) buf.push_back({v});
                }
                // in-neighbors
                for (uint64_t e = in_offset[u]; e < in_offset[u + 1]; e++) {
                    uint32_t v = in_source[e];
                    if (v != u) buf.push_back({v});
                }

                if (buf.empty()) {
                    xadj64[u + 1] = xadj64[u];
                    continue;
                }// Output new_prefix (size = total_block+1), and guarantee boundary %64==0 for p < last

                std::sort(buf.begin(), buf.end(),
                        [](const Nbr& a, const Nbr& b) { return a.v < b.v; });

                uint64_t uniq = 0;
                for (size_t i = 0; i < buf.size();) {
                    size_t j = i + 1;
                    while (j < buf.size() && buf[j].v == buf[i].v) j++;
                    uniq++;
                    i = j;
                }

                xadj64[u + 1] = xadj64[u] + uniq;
            }
        }

        const uint64_t adj_size64 = xadj64[meta.total_v];
        VLOG(1) << "METIS CSR: n=" << meta.total_v << ", adj_size64=" << adj_size64;

        // idx_t range check (system compatibility)
        const uint64_t idx_max_u64 =
            (sizeof(idx_t) == 8)
                ? static_cast<uint64_t>(std::numeric_limits<int64_t>::max())
                : static_cast<uint64_t>(std::numeric_limits<int32_t>::max());

        if (adj_size64 > idx_max_u64) {
            LOG(FATAL)
                << "metis_partition: adj_size (" << adj_size64 << ") exceeds idx_t max ("
                << idx_max_u64 << ").\n"
                << "Your system METIS is IDXTYPEWIDTH=" << IDXTYPEWIDTH
                << " (likely 32-bit). For this graph, you MUST use 64-bit METIS "
                << "(build METIS with IDX64/IDXTYPEWIDTH=64 and link against it).";
        }

        if (adj_size64 > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
            LOG(FATAL) << "metis_partition: adj_size too large for size_t on this platform.";
        }

        // Convert xadj64 -> xadj (idx_t)
        std::vector<idx_t> xadj(static_cast<size_t>(meta.total_v) + 1, 0);

        for (uint32_t i = 0; i <= meta.total_v; i++) {
            xadj[i] = static_cast<idx_t>(xadj64[i]);
        }

        const idx_t adj_size = static_cast<idx_t>(adj_size64);

        // Allocate adjncy
        std::vector<idx_t> adjncy;
        try {
            adjncy.resize(static_cast<size_t>(adj_size));
        } catch (const std::exception& e) {
            LOG(FATAL)
                << "metis_partition: failed to allocate adjncy of size " << (uint64_t)adj_size
                << " (" << (uint64_t)adj_size * sizeof(idx_t) << " bytes). Exception: " << e.what()
                << "\nLikely insufficient RAM. Consider: (1) 64-bit METIS, (2) more memory, "
                << "(3) build METIS graph with fewer neighbors (e.g., only out-neighbors), "
                << "(4) use a scalable partitioner (ParMETIS/KaHIP).";
        }

        // Fill adjncy
        {
            std::vector<Nbr> buf;
            idx_t pos = 0;

            for (uint32_t u = 0; u < meta.total_v; u++) {
                buf.clear();

                uint64_t want = (uint64_t)out_degree[u] + (uint64_t)in_degree[u];
                if (want > 0) {
                    uint64_t cap = std::min<uint64_t>(want, 50ull * 1000ull * 1000ull);
                    buf.reserve((size_t)cap);
                }

                for (uint64_t e = out_offset[u]; e < out_offset[u + 1]; e++) {
                    uint32_t v = out_dest[e];
                    if (v != u) buf.push_back({v});
                }
                for (uint64_t e = in_offset[u]; e < in_offset[u + 1]; e++) {
                    uint32_t v = in_source[e];
                    if (v != u) buf.push_back({v});
                }

                if (!buf.empty()) {
                    std::sort(buf.begin(), buf.end(),
                            [](const Nbr& a, const Nbr& b) { return a.v < b.v; });

                    for (size_t i = 0; i < buf.size();) {
                        uint32_t v = buf[i].v;
                        size_t j = i + 1;
                        while (j < buf.size() && buf[j].v == v) j++;

                        if (static_cast<uint64_t>(pos) >= adj_size64) {
                            LOG(FATAL) << "metis_partition: pos overflow while filling adjncy. "
                                    << "pos=" << (int64_t)pos << ", adj_size=" << (uint64_t)adj_size64;
                        }
                        adjncy[static_cast<size_t>(pos++)] = static_cast<idx_t>(v);
                        i = j;
                    }
                }
            }

            if (static_cast<uint64_t>(pos) != adj_size64) {
                LOG(FATAL) << "metis_partition: CSR mismatch, pos=" << (int64_t)pos
                        << ", adj_size=" << (uint64_t)adj_size64;
            }
        }

        t_build.from_tick();

        // Run METIS
        timer t_metis;
        t_metis.tick("metis_compute");

        idx_t nvtxs = n;
        idx_t ncon  = 1;
        idx_t objval = 0;

        std::vector<idx_t> part(static_cast<size_t>(n), 0);

        idx_t options[METIS_NOPTIONS];
        METIS_SetDefaultOptions(options);
        options[METIS_OPTION_NUMBERING] = 0;
        options[METIS_OPTION_SEED] = 42;

        int status = METIS_PartGraphKway(
            &nvtxs,
            &ncon,
            xadj.data(),
            adjncy.data(),
            nullptr,   // vwgt
            nullptr,   // vsize
            nullptr,   // adjwgt
            &nparts,
            nullptr,
            nullptr,
            options,
            &objval,
            part.data()
        );

        if (status != METIS_OK) {
            LOG(FATAL) << "METIS_PartGraphKway failed, status=" << status;
        }

        t_metis.from_tick();

        // Reorder vertices by METIS parts (chunk-aligned)
        timer t_reorder;
        t_reorder.tick("metis_reorder");

        std::vector<uint32_t> part_u32(meta.total_v);

        for (uint32_t i = 0; i < meta.total_v; i++) {
            part_u32[i] = static_cast<uint32_t>(part[static_cast<size_t>(i)]);
        }

        std::vector<uint32_t> metis_prefix;
        reorder_vertices_by_metis_parts(part_u32, total_block, metis_prefix);

        t_reorder.from_tick();

        // Build 1D partition outgoing-edge statistics AFTER reordering (O(V))
        std::vector<uint64_t> part_out_edges(static_cast<size_t>(total_block), 0);

        for (int p = 0; p < total_block; p++) {
            uint32_t begin = metis_prefix[p];
            uint32_t end   = metis_prefix[p + 1];

            if (begin >= meta.total_v) continue;
            if (end > meta.total_v) end = meta.total_v;

            uint64_t sum = 0;
            for (uint32_t u = begin; u < end; u++) {
                sum += (out_offset[u + 1] - out_offset[u]);
            }
            part_out_edges[static_cast<size_t>(p)] = sum;
        }

        // Build partition_result (1D)
        for (int p = 0; p < total_block; p++) {
            uint64_t e64 = part_out_edges[static_cast<size_t>(p)];
            uint32_t e32 = (e64 > UINT32_MAX) ? UINT32_MAX : static_cast<uint32_t>(e64);
            result.add(metis_prefix[p], metis_prefix[p + 1], 0, meta.total_v, e32);
        }

        t_total.from_tick();
        return result;
    }


    partition_result generate_checkerboard_partition_from_cuts(int cut, std::vector<uint32_t> cuts) {
        for (int i = 0; i < (int)cuts.size() - 1; i++) {
            cuts[i] = cuts[i] / 64 * 64;
        }
        VLOG(1) << "aligned cuts: " << log_array<uint32_t>(cuts.data(), uint64_t(cuts.size())).str();
        partition_result result;
        #pragma omp parallel for
        for (int t = 0; t < cut; t++) {
            std::vector<uint32_t> block_edges(cut);
            for (uint32_t i = cuts[t]; i < cuts[t + 1]; i++) {
                for (uint64_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                    uint32_t dest = out_dest[j];
                    for (int k = 0; k < cut; k++) {
                        if (dest >= cuts[k] && dest < cuts[k + 1]) {
                            block_edges[k]++;
                            break;
                        }
                    }
                }
            }
            #pragma omp critical
            {
                for (int i = 0; i < cut; i++) {
                    result.add(cuts[t], cuts[t + 1], cuts[i], cuts[i + 1], block_edges[i]);
                }
            }
        }
        return result;
    }

    partition_result naive_checkerboard_partition(int cut) {
        timer t;
        t.tick("partition time");
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
        t.from_tick();
        return generate_checkerboard_partition_from_cuts(cut, cuts);
    }

    std::vector<uint64_t> generate_workload_limit_check_list(uint64_t left, uint64_t right) {
        int total_thread = omp_get_max_threads();
        std::vector<uint64_t> check_list;
        if (left == right || (right - left - 1) < uint64_t(total_thread)) {
            check_list.push_back(left + (right - left) / 2);
        } else {
            uint64_t step = (right - left - 1) / total_thread, p = left;
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
        if (cut == 1) {
            std::vector<uint32_t> cuts = {0, meta.total_v};
            return generate_checkerboard_partition_from_cuts(cut, cuts);
        }
        timer t;
        t.tick("partition time");
        uint64_t left = 0, right = meta.total_e + meta.total_v * 2 * COMM_COMP_RATIO;
        std::vector<uint32_t> result_cuts(cut + 1, 0);
        while ((double)(right - left) / right >= 0.01 || result_cuts[cut - 1] == 0) {
            VLOG(1) << "left: " << left << ", right: " << right;
            std::vector<uint64_t> check_list = generate_workload_limit_check_list(left, right);
            #pragma omp parallel for
            for (int t = 0; t < (int)check_list.size(); t++) {
                std::vector<uint32_t> cuts, in_workload(cut), out_workload(cut);
                std::vector<uint64_t> workload(cut * 2 - 1);
                cuts.push_back(0);
                uint64_t workload_limit = check_list[t];
                bool plan_satisfy_limit = true;
                for (uint32_t i = 0; i < meta.total_v; i++) {
                    int current_cut = cuts.size(), diagonal = 0;
                    std::fill(in_workload.begin(), in_workload.end(), 0);
                    std::fill(out_workload.begin(), out_workload.end(), 0);
                    for (uint64_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                        uint32_t dest = out_dest[j];
                        if (dest == i) diagonal = 1;
                        if (dest >= i) continue;
                        for (int k = 0; k < current_cut; k++) {
                            if (k == current_cut - 1) {
                                out_workload[k]++;
                                break;
                            } else if (cuts[k] <= dest && dest < cuts[k + 1]) {
                                out_workload[k]++;
                                break;
                            }
                        }
                    }
                    for (uint64_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
                        uint32_t source = in_source[j];
                        if (source >= i) continue;
                        for (int k = 0; k < current_cut; k++) {
                            if (k == current_cut - 1) {
                                in_workload[k]++;
                                break;
                            } else if (cuts[k] <= source && source < cuts[k + 1]) {
                                in_workload[k]++;
                                break;
                            }
                        }
                    }
                    for (int j = 0; j < current_cut; j++) {
                        in_workload[j] += COMM_COMP_RATIO;
                        out_workload[j] += COMM_COMP_RATIO;
                    }
                    bool block_satisfy_limit = true;
                    for (int j = 0; j < current_cut - 1; j++)
                        if (workload[j] + uint64_t(out_workload[j]) > workload_limit) {
                            block_satisfy_limit = false;
                            break;
                        }
                    for (int j = 0; j < current_cut - 1; j++)
                        if (workload[current_cut * 2 - 2 - j] + uint64_t(in_workload[j]) > workload_limit) {
                            block_satisfy_limit = false;
                            break;
                        }
                    if (current_cut > 0) 
                        if (workload[current_cut - 1] + uint64_t(in_workload[current_cut - 1] + out_workload[current_cut - 1] + diagonal) > workload_limit)
                            block_satisfy_limit = false;
                    if (!block_satisfy_limit) {
                        cuts.push_back(i);
                        if ((int)cuts.size() == cut + 1) {
                            plan_satisfy_limit = false;
                            break;
                        }
                        current_cut++;
                        for (int j = 0; j < current_cut - 1; j++) {
                            workload[j] = (uint64_t)(cuts[j + 1] - cuts[j]) * COMM_COMP_RATIO;
                        }
                        for (int j = 0; j < current_cut - 1; j++) {
                            workload[current_cut * 2 - 2 - j] = (uint64_t)(cuts[j + 1] - cuts[j]) * COMM_COMP_RATIO;
                        }
                        workload[current_cut - 1] = 0;
                    }
                    for (int j = 0; j < current_cut; j++) {
                        workload[j] += uint64_t(out_workload[j]);
                    }
                    for (int j = 0; j < current_cut; j++) {
                        workload[current_cut * 2 - 2 - j] += uint64_t(in_workload[j]);
                    }
                    workload[current_cut - 1] += uint64_t(diagonal);
                }
                #pragma omp critical 
                {
                    if (plan_satisfy_limit) {
                        if (workload_limit >= left && workload_limit <= right) {
                            for (int j = 0; j < (int)cuts.size(); j++)   
                                result_cuts[j] = cuts[j];
                            right = workload_limit - 1;
                        }
                    } else {
                        if (workload_limit >= left && workload_limit <= right) {
                            left = workload_limit + 1;
                        }
                    }
                }
            }
        }
        result_cuts[cut] = meta.total_v;
        t.from_tick();
        return generate_checkerboard_partition_from_cuts(cut, result_cuts);
    }

    std::vector<graph_set<ewT> *> partition(partition_result result, bool clean) {
        std::vector<graph<ewT> *> subgraphs;
        for (auto block: result.blocks){
            if (block.edges != 0 || block.root() || !clean) {
                subgraphs.push_back(new graph<ewT>(block, meta));
            }
        }
        #pragma omp parallel for
        for (int t = 0; t < (int)subgraphs.size(); t++) {
            graph<ewT> *subgraph = subgraphs[t];
            for (uint32_t i = subgraph -> from_dest; i < subgraph -> to_dest; i++) {
                subgraph -> begin_add_edge(i, EDGE_DIRECTION::INCOMING);
                for (uint64_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
                    uint32_t source = in_source[j];
                    if (source >= subgraph -> from_source && source < subgraph -> to_source) {
                        if (!weighted) {
                            subgraph -> add_edge(source, i, EDGE_DIRECTION::INCOMING);
                        } else {
                            ewT weight = in_weight[j];
                            subgraph -> add_edge(source, i, weight, EDGE_DIRECTION::INCOMING);
                        }
                    }
                }
            }
            subgraph -> end_add_edge(EDGE_DIRECTION::INCOMING);
        }
        #pragma omp parallel for
        for (int t = 0; t < (int)subgraphs.size(); t++) {
            graph<ewT> *subgraph = subgraphs[t];
            for (uint32_t i = subgraph -> from_source; i < subgraph -> to_source; i++) {
                subgraph -> begin_add_edge(i, EDGE_DIRECTION::OUTGOING);
                for (uint64_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
                    uint32_t dest = out_dest[j];
                    if (dest >= subgraph -> from_dest && dest < subgraph -> to_dest) {
                        if (!weighted) {
                            subgraph -> add_edge(i, dest, EDGE_DIRECTION::OUTGOING);
                        } else {
                            ewT weight = out_weight[j];
                            subgraph -> add_edge(i, dest, weight, EDGE_DIRECTION::OUTGOING);
                        }
                    }
                }
            }
            subgraph -> end_add_edge(EDGE_DIRECTION::OUTGOING);
        }
        for (int i = 0; i < (int)subgraphs.size(); i++) {
            subgraphs[i] -> set_in_degree(in_degree + subgraphs[i] -> from_dest);
            subgraphs[i] -> set_out_degree(out_degree + subgraphs[i] -> from_source);
        }
        for (int i = 0; i < (int)subgraphs.size(); i++) {
            subgraphs[i] -> check();
        }
        std::vector<graph_set<ewT> *> graphsets(subgraphs.size(), nullptr);
        #pragma omp parallel for
        for (int i = 0; i < (int)subgraphs.size(); i++) {
            graph_set<ewT> *graphset = new graph_set<ewT>(subgraphs[i], meta);
            #pragma omp critical
            {
                graphsets[i] = graphset;
            }
        }
        return graphsets;
    }

    void print() {
        VLOG(1) << "Total V: " << meta.total_v;
        VLOG(1) << "Total E: " << meta.total_e;
        VLOG(2) << "In Offset: " << log_array<uint64_t>(in_offset, uint64_t(meta.total_v + 1)).str();
        VLOG(2) << "In Source: " << log_array<uint32_t>(in_source, meta.total_e).str();
        VLOG(2) << "Out Offset: " << log_array<uint64_t>(out_offset, uint64_t(meta.total_v + 1)).str();
        VLOG(2) << "Out Dest: " << log_array<uint32_t>(out_dest, meta.total_e).str();
    }

};

#endif
