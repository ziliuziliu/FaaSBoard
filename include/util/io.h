#ifndef _UTILS_H
#define _UTILS_H

#include "log.h"

#include <iostream>
#include <fstream>
#include <string>

void parse_line(char *line, int line_size, uint32_t *x, uint32_t *y) {
    *x = *y = 0;
    bool first_num = true;
    for (int i = 0; i < line_size; i++) {
        if (line[i] >= '0' && line[i] <= '9') {
            if (first_num) *x = *x * 10 + line[i] - '0';
            else *y = *y * 10 + line[i] - '0';
        } else {
            if (first_num) first_num = false;
            else break;
        }
    }
}

void vertex_hash(uint32_t *mapping, uint32_t mx, uint32_t *edge_buffer, uint64_t edge_count) {
    for (uint32_t i = 1; i <= mx; i++)
        mapping[i] += mapping[i - 1];
    for (uint32_t i = 0; i <= mx; i++)
        mapping[i]--;
    #pragma omp parallel for
    for (uint64_t i = 0; i < edge_count; i++)
        edge_buffer[i] = mapping[edge_buffer[i]];
}

// w = (u + v) % 100
template <class T>
void read_txt_util(std::string path, uint32_t *in_offset, uint32_t *in_source, uint32_t *out_offset, uint32_t *out_dest, 
    T *in_weight, T *out_weight, bool weighted, uint32_t total_v, uint32_t total_e) {
    LOG(INFO) << "start reading txt";
    FILE *txt_file = fopen(path.c_str(), "r");
    char *line = new char[100]; 
    size_t line_size = 0;
    uint64_t edge_buffer_count = 0;
    uint32_t mx = 0, x, y;
    uint32_t *mapping = new uint32_t[total_v * 3];
    uint32_t *edge_buffer = new uint32_t[uint64_t(total_e) * 2];
    while (getline(&line, &line_size, txt_file) > 0) {
        if (line[0] == '#') continue;
        parse_line(line, line_size, &x, &y);
        edge_buffer[edge_buffer_count++] = x;
        edge_buffer[edge_buffer_count++] = y;
        if (edge_buffer_count % 10000000 == 0) {
            LOG(INFO) << "current size " << edge_buffer_count;
        }
        mapping[x] = mapping[y] = 1;
        mx = std::max(mx, std::max(x, y));
    }
    fclose(txt_file);
    LOG(INFO) << "have read everything, size " << edge_buffer_count / 2;
    LOG(INFO) << "start hashing";
    vertex_hash(mapping, mx, edge_buffer, edge_buffer_count);
    LOG(INFO) << "start building csr";
    for (uint64_t i = 0; i < edge_buffer_count; i += 2) {
        uint32_t u = edge_buffer[i], v = edge_buffer[i + 1];
        in_offset[v + 1]++; 
        out_offset[u + 1]++;
    }
    for (uint32_t i = 1; i <= total_v; i++) {
        in_offset[i] += in_offset[i - 1];
        out_offset[i] += out_offset[i - 1];
    }
    for (uint64_t i = 0; i < edge_buffer_count; i += 2) {
        uint32_t u = edge_buffer[i], v = edge_buffer[i + 1];
        in_source[in_offset[v]] = u;
        out_dest[out_offset[u]] = v;
        if (weighted) {
            in_weight[in_offset[v]] = (T)(uint64_t)((u + v) % 100);
            out_weight[out_offset[u]] = (T)(uint64_t)((u + v) % 100);
        }
        in_offset[v]++;
        out_offset[u]++;
    }
    for (uint32_t i = total_v; i > 0; i--) {
        in_offset[i] = in_offset[i - 1];
        out_offset[i] = out_offset[i - 1];
    }
    in_offset[0] = out_offset[0] = 0;
    delete [] edge_buffer;
    delete [] mapping;
    LOG(INFO) << "end reading txt";
}

template <class T>
void save_metis_util(std::string path, uint32_t *in_offset, uint32_t *in_source, uint32_t *out_offset, uint32_t *out_dest, 
    T *in_weight, T *out_weight, bool weighted, uint32_t total_v, uint32_t total_e) {
    LOG(INFO) << "start saving metis";
    FILE *metis_file = fopen(path.c_str(), "w");
    fprintf(metis_file, "%u %u\n", total_v, total_e);
    for (uint32_t i = 0; i < total_v; i++) {
        if (i % 1000000 == 0) {
            LOG(INFO) << "save " << i << " vertex";
        }
        for (uint32_t j = in_offset[i]; j < in_offset[i + 1]; j++) {
            fprintf(metis_file, "%u ", in_source[j] + 1);
        }
        for (uint32_t j = out_offset[i]; j < out_offset[i + 1]; j++) {
            fprintf(metis_file, "%u ", out_dest[j] + 1);
        }
        fprintf(metis_file, "\n");
    }
    fclose(metis_file);
    LOG(INFO) << "end saving metis";
}

template <class T>
void read_csr_util(std::string path, uint32_t *in_offset, uint32_t *in_source, uint32_t *out_offset, uint32_t *out_dest, 
    T *in_weight, T *out_weight, bool weighted, uint32_t in_vertex_cnt, uint32_t out_vertex_cnt, uint32_t total_e) {
    LOG(INFO) << "start reading csr";
    FILE *csr_file = fopen(path.c_str(), "rb");
    size_t size;
    size = fread(in_offset, 4, in_vertex_cnt + 1, csr_file);
    CHECK(size == in_vertex_cnt + 1) << "fread failed";
    size = fread(in_source, 4, total_e, csr_file);
    CHECK(size == total_e) << "fread failed";
    size = fread(out_offset, 4, out_vertex_cnt + 1, csr_file);
    CHECK(size == out_vertex_cnt + 1) << "fread failed";
    size = fread(out_dest, 4, total_e, csr_file);
    CHECK(size == total_e) << "fread failed";
    if (weighted) {
        size = fread(in_weight, sizeof(T), total_e, csr_file);
        CHECK(size == total_e) << "fread failed";
        size = fread(out_weight, sizeof(T), total_e, csr_file);
        CHECK(size == total_e) << "fread failed";
    }
    fclose(csr_file);
    LOG(INFO) << "end reading csr";
}

template <class T>
void save_csr_util(std::string path, uint32_t *in_offset, uint32_t *in_source, uint32_t *out_offset, uint32_t *out_dest, 
    T *in_weight, T *out_weight, bool weighted, uint32_t in_vertex_cnt, uint32_t out_vertex_cnt, uint32_t total_e) {
    LOG(INFO) << "start saving csr";
    FILE *csr_file = fopen(path.c_str(), "wb");
    size_t size;
    size = fwrite(in_offset, 4, in_vertex_cnt + 1, csr_file);
    CHECK(size == in_vertex_cnt + 1) << "fwrite failed";
    size = fwrite(in_source, 4, total_e, csr_file);
    CHECK(size == total_e) << "fwrite failed";
    size = fwrite(out_offset, 4, out_vertex_cnt + 1, csr_file);
    CHECK(size == out_vertex_cnt + 1) << "fwrite failed";
    size = fwrite(out_dest, 4, total_e, csr_file);
    CHECK(size == total_e) << "fwrite failed";
    if (weighted) {
        size = fwrite(in_weight, sizeof(T), total_e, csr_file);
        CHECK(size == total_e) << "fwrite failed";
        size = fwrite(out_weight, sizeof(T), total_e, csr_file);
        CHECK(size == total_e) << "fwrite failed";
    }
    fclose(csr_file);
    LOG(INFO) << "end saving csr";
}

template<class T>
void save_result_util(std::ofstream &file, uint32_t start, uint32_t end, T *vec) {
    for (uint32_t v = start; v < end; v++) {
        T val = vec[v - start];
        file << v << ": " << val << std::endl;
    }
}

#endif