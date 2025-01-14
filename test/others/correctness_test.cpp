#include "preprocess/raw_graph.h"
#include "util/log.h"
#include "util/flags.h"
#include "util/s3.h"

#include <cstring>
#include <queue>
#include <filesystem>
#include <chrono>
#include <omp.h>

uint32_t *bfs(raw_graph<empty> *g, uint32_t total_v, uint32_t bfs_root) {
    uint32_t *dis = new uint32_t[total_v];
    for (uint32_t i = 0; i < total_v; i++) {
        dis[i] = 0xffffffff;
    }
    dis[bfs_root] = 0;
    std::queue<uint32_t> q;
    q.push(bfs_root);
    while (!q.empty()) {
        uint32_t x = q.front();
        q.pop();
        for (uint32_t i = g -> out_offset[x]; i < g -> out_offset[x + 1]; i++) {
            uint32_t v = g -> out_dest[i];
            if (dis[v] == 0xffffffff) {
                dis[v] = dis[x] + 1;
                q.push(v);
            }
        }
    }
    VLOG(1) << "correct top 100: " << log_array<uint32_t>(dis, 100).str();
    return dis;
}

float *pagerank(raw_graph<empty> *g, uint32_t total_v, int iterations) {
    float *cur_pr = new float[total_v], *nxt_pr = new float[total_v];
    for (uint32_t i = 0; i < total_v; i++) {
        uint32_t out_d = g -> out_offset[i + 1] - g -> out_offset[i];
        cur_pr[i] = out_d > 0 ? 1.0 / out_d : 1.0;
        nxt_pr[i] = 0.0;
    }
    for (int it = 1; it <= iterations; it++) {
        VLOG(1) << "pagerank iteration " << it;
        #pragma omp parallel for schedule(dynamic, 1000)
        for (uint32_t dst = 0; dst < total_v; dst++) {
            nxt_pr[dst] = 0.0;
            for (uint32_t i = g -> in_offset[dst]; i < g -> in_offset[dst + 1]; i++) {
                uint32_t src = g -> in_source[i];
                nxt_pr[dst] += cur_pr[src];
            }
            nxt_pr[dst] = 0.15 + 0.85 * nxt_pr[dst];
            if (it != iterations) {
                uint32_t out_d = g -> out_offset[dst + 1] - g -> out_offset[dst];
                nxt_pr[dst] = out_d > 0 ? nxt_pr[dst] / out_d : nxt_pr[dst];
            }
        }
        std::swap(cur_pr, nxt_pr);
    }
    VLOG(1) << "correct top 100: " << log_array<float>(cur_pr, 100).str();
    return cur_pr;
}

template <class T>
T *read_result_from_file(std::string result_dir, uint32_t total_v) {
    T *result = new T[total_v];
    for (const auto &entry : fs::directory_iterator(result_dir)) {
        check_result_freshness(entry.path());
        read_result_util<T>(entry.path(), result);
    }
    VLOG(1) << "result top 100: " << log_array<T>(result, 100).str();
    return result;
}

void read_result_from_s3() {
    std::vector<std::string> objects = s3_list_objects(FLAGS_s3_bucket);
    for (auto object : objects) {
        s3_check_object_freshness(FLAGS_s3_bucket, object);
        s3_get_object_to_file(FLAGS_s3_bucket, object, FLAGS_result_dir + "/" + object);
    }
}

template <class T>
T *read_result(CAAS_SAVE_MODE save_mode, std::string result_dir, uint32_t total_v) {
    switch (save_mode) {
        case CAAS_SAVE_MODE::NO_SAVE:
            break;
        case CAAS_SAVE_MODE::SAVE_LOCAL:
            return read_result_from_file<T>(result_dir, total_v);
        case CAAS_SAVE_MODE::SAVE_S3:
            read_result_from_s3();
            return read_result_from_file<T>(result_dir, total_v);
        default:
            LOG(FATAL) << "save mode " << (int)save_mode << " not implemented";
    }
    return nullptr;
}

template <class T>
void check_equal(uint32_t total_v, T *vec1, T *vec2) {
    for (uint32_t i = 0; i < total_v; i++) {
        CHECK(vec1[i] == vec2[i]) << "vertex " << i << " correct " << vec1[i] << " result " << vec2[i];
    }
}

template <class T>
void check_error(uint32_t total_v, T *vec1, T *vec2, double error_rate) {
    for (uint32_t i = 0; i < total_v; i++) {
        CHECK(std::abs((double)vec1[i] - (double)vec2[i]) / vec1[i] <= error_rate) << "vertex " << i << " correct " << vec1[i] << " result " << vec2[i];
    }
}

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    if ((CAAS_SAVE_MODE)FLAGS_save_mode == CAAS_SAVE_MODE::SAVE_S3) {
        s3_init();
    }
    if (FLAGS_application == "bfs") {
        uint32_t *dis2 = read_result<uint32_t>((CAAS_SAVE_MODE)FLAGS_save_mode, FLAGS_result_dir, FLAGS_vertices);
        raw_graph<empty> g(FLAGS_vertices, FLAGS_edges);
        g.read_txt(FLAGS_graph_file, false);
        uint32_t *dis1 = bfs(&g, FLAGS_vertices, FLAGS_bfs_root);
        check_equal<uint32_t>(FLAGS_vertices, dis1, dis2);
    } else if (FLAGS_application == "pr") {
        float *pr2 = read_result<float>((CAAS_SAVE_MODE)FLAGS_save_mode, FLAGS_result_dir, FLAGS_vertices);
        raw_graph<empty> g(FLAGS_vertices, FLAGS_edges);
        g.read_txt(FLAGS_graph_file, false);
        float *pr1 = pagerank(&g, FLAGS_vertices, FLAGS_pr_iterations);
        check_error<float>(FLAGS_vertices, pr1, pr2, 0.01);
    }
    VLOG(1) << "Freshness Check (<=30min) Passed";   
    VLOG(1) << "Correctness Check Passed";
    return 0;
}