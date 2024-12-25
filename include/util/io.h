#ifndef _UTILS_H
#define _UTILS_H

#include "log.h"

#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>
#include <chrono>

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
void read_txt_util(
    std::string path, bool undirected,
    uint32_t *in_offset, uint32_t *in_source, T *in_weight, uint32_t *in_degree, 
    uint32_t *out_offset, uint32_t *out_dest, T *out_weight, uint32_t *out_degree, 
    bool weighted, uint32_t total_v, uint32_t total_e
) {
    VLOG(1) << "start reading txt";
    FILE *txt_file = fopen(path.c_str(), "r");
    char *line = new char[100]; 
    size_t line_size = 0;
    uint64_t edge_buffer_count = 0;
    uint32_t mx = 0, x, y;
    uint32_t *mapping = new uint32_t[total_v * 3];
    uint32_t *edge_buffer = new uint32_t[(uint64_t)total_e * 2];
    while (getline(&line, &line_size, txt_file) > 0) {
        if (line[0] == '#') continue;
        parse_line(line, line_size, &x, &y);
        edge_buffer[edge_buffer_count++] = x;
        edge_buffer[edge_buffer_count++] = y;
        if (undirected) {
            edge_buffer[edge_buffer_count++] = y;
            edge_buffer[edge_buffer_count++] = x;
        }
        if (edge_buffer_count <= 20) {
            VLOG(1) << x << " " << y;
        }
        if (edge_buffer_count % 10000000 == 0) {
            VLOG(1) << "current size " << edge_buffer_count;
        }
        mapping[x] = mapping[y] = 1;
        mx = std::max(mx, std::max(x, y));
    }
    fclose(txt_file);
    VLOG(1) << "have read everything, size " << edge_buffer_count / 2;
    VLOG(1) << "start hashing";
    vertex_hash(mapping, mx, edge_buffer, edge_buffer_count);
    VLOG(1) << "start building csr";
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
    for (uint32_t i = 0; i < total_v; i++) {
        in_degree[i] = in_offset[i + 1] - in_offset[i];
        out_degree[i] = out_offset[i + 1] - out_offset[i];
    }
    delete [] edge_buffer;
    delete [] mapping;
    VLOG(1) << "end reading txt";
}

template <class T>
void read_csr_util(
    std::string in_path, std::string out_path,
    uint32_t *in_offset, uint32_t *in_source, T *in_weight, uint32_t *in_degree, 
    uint32_t *out_offset, uint32_t *out_dest, T *out_weight, uint32_t *out_degree, 
    bool weighted, bool only_in, bool only_out, uint32_t in_vertex_cnt, uint32_t out_vertex_cnt, uint32_t total_e
) {
    VLOG(1) << "start reading csr";
    size_t size;
    FILE *in_degree_file = fopen((in_path + ".deg").c_str(), "rb");
    size = fread(in_degree, 4, out_vertex_cnt, in_degree_file);
    CHECK(size == out_vertex_cnt) << "fread failed, read size " << size << " actual " << out_vertex_cnt;
    fclose(in_degree_file);
    if (!only_out) {
        FILE *in_csr_file = fopen(in_path.c_str(), "rb");
        size = fread(in_offset, 4, out_vertex_cnt + 1, in_csr_file);
        CHECK(size == out_vertex_cnt + 1) << "fread failed, read size " << size << " actual " << out_vertex_cnt + 1;
        size = fread(in_source, 4, total_e, in_csr_file);
        CHECK(size == total_e) << "fread failed, read size " << size << " actual " << total_e;
        if (weighted) {
            size = fread(in_weight, sizeof(T), total_e, in_csr_file);
            CHECK(size == total_e) << "fread failed, read size " << size << " actual " << total_e;
        }
        fclose(in_csr_file);
    }
    FILE *out_degree_file = fopen((out_path + ".deg").c_str(), "rb");
    size = fread(out_degree, 4, in_vertex_cnt, out_degree_file);
    CHECK(size == in_vertex_cnt) << "fread failed, read size " << size << " actual " << in_vertex_cnt;
    fclose(out_degree_file);
    if (!only_in) {
        FILE *out_csr_file = fopen(out_path.c_str(), "rb");
        size = fread(out_offset, 4, in_vertex_cnt + 1, out_csr_file);
        CHECK(size == in_vertex_cnt + 1) << "fread failed, read size " << size << " actual " << in_vertex_cnt + 1;
        size = fread(out_dest, 4, total_e, out_csr_file);
        CHECK(size == total_e) << "fread failed, read size " << size << " actual " << total_e;
        if (weighted) {
            size = fread(out_weight, sizeof(T), total_e, out_csr_file);
            CHECK(size == total_e) << "fread failed, read size " << size << " actual " << total_e;
        }
        fclose(out_csr_file);
    }
    VLOG(1) << "end reading csr";
}

template <class T>
void save_csr_util(
    std::string in_path, std::string out_path, 
    uint32_t *in_offset, uint32_t *in_source, T *in_weight, uint32_t *in_degree, 
    uint32_t *out_offset, uint32_t *out_dest, T *out_weight, uint32_t *out_degree, 
    bool weighted, uint32_t in_vertex_cnt, uint32_t out_vertex_cnt, uint32_t total_e
) {
    VLOG(1) << "start saving csr";
    size_t size;
    FILE *in_degree_file = fopen((in_path + ".deg").c_str(), "wb");
    size = fwrite(in_degree, 4, out_vertex_cnt, in_degree_file);
    CHECK(size == out_vertex_cnt) << "fwrite failed, read size " << size << " actual " << out_vertex_cnt;
    fclose(in_degree_file);
    FILE *in_csr_file = fopen(in_path.c_str(), "wb");
    size = fwrite(in_offset, 4, out_vertex_cnt + 1, in_csr_file);
    CHECK(size == out_vertex_cnt + 1) << "fwrite failed, write size " << size << " actual " << out_vertex_cnt + 1;
    size = fwrite(in_source, 4, total_e, in_csr_file);
    CHECK(size == total_e) << "fwrite failed, write size " << size << " actual " << total_e;
    if (weighted) {
        size = fwrite(in_weight, sizeof(T), total_e, in_csr_file);
        CHECK(size == total_e) << "fwrite failed, write size " << size << " actual " << total_e;
    }
    fclose(in_csr_file);
    FILE *out_degree_file = fopen((out_path + ".deg").c_str(), "wb");
    size = fwrite(out_degree, 4, in_vertex_cnt, out_degree_file);
    CHECK(size == in_vertex_cnt) << "fwrite failed, read size " << size << " actual " << in_vertex_cnt;
    fclose(out_degree_file);
    FILE *out_csr_file = fopen(out_path.c_str(), "wb");
    size = fwrite(out_offset, 4, in_vertex_cnt + 1, out_csr_file);
    CHECK(size == in_vertex_cnt + 1) << "fwrite failed, write size " << size << " actual " << in_vertex_cnt + 1;
    size = fwrite(out_dest, 4, total_e, out_csr_file);
    CHECK(size == total_e) << "fwrite failed, write size " << size << " actual " << total_e;
    if (weighted) {
        size = fwrite(out_weight, sizeof(T), total_e, out_csr_file);
        CHECK(size == total_e) << "fwrite failed, write size " << size << " actual " << total_e;
    }
    fclose(out_csr_file);
    VLOG(1) << "end saving csr";
}

void check_result_freshness(std::string result_path) {
    auto last_write_time = std::filesystem::last_write_time(result_path);
    auto last_write_time_system = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
        last_write_time - decltype(last_write_time)::clock::now() + std::chrono::system_clock::now()
    );
    auto current_time = std::chrono::system_clock::now();
    auto diff_time = std::chrono::duration_cast<std::chrono::minutes>(current_time - last_write_time_system).count();
    CHECK(diff_time <= 30) << "result file is generated long time ago";
}

template <class T>
void read_result_util(std::string result_path, T *vec) {
    std::ifstream result_file(result_path);
    if (!result_file.is_open()) {
        LOG(FATAL) << "unable to open file " << result_path;
    }
    std::string line;
    while (std::getline(result_file, line)) {
        std::istringstream iss(line);
        uint32_t a;
        T b;
        if (iss >> a >> b) {
            vec[a] = b;
        } else {
            LOG(FATAL) << "error parsing line " << line;
        }
    }
}

template<class T>
void save_result_util(std::string result_path, uint32_t start, uint32_t end, T *vec) {
    std::filesystem::remove(result_path);
    std::ofstream result_file(result_path);
    if (!result_file.is_open()) {
        LOG(FATAL) << "unable to open file " << result_path;
    }
    for (uint32_t v = start; v < end; v++) {
        result_file << v << " " << vec[v - start] << std::endl;
    }
    result_file.close();
}

#endif