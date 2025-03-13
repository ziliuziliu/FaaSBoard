#include "preprocess/raw_graph.h"
#include "util/log.h"
#include "util/flags.h"
#include "util/sdk.h"

#include <cstring>
#include <queue>
#include <filesystem>
#include <chrono>
#include <omp.h>

s3_sdk *s_sdk;

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
    VLOG(1) << "correct top 100: " << log_array<uint32_t>(dis, uint64_t(100)).str();
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
    VLOG(1) << "correct top 100: " << log_array<float>(cur_pr, uint64_t(100)).str();
    return cur_pr;
}

template <typename T>
T* sssp(raw_graph<T>* g, uint32_t total_v, uint32_t sssp_root) {
    T* dis = new T[total_v];
    for (uint32_t i = 0; i < total_v; i++) {
        dis[i] = std::numeric_limits<T>::max();
    }
    dis[sssp_root] = static_cast<T>(0); 

    std::queue<uint32_t> q;
    q.push(sssp_root);

    while (!q.empty()) {
        uint32_t u = q.front();
        q.pop();

        for (uint32_t i = g -> out_offset[u]; i < g -> out_offset[u + 1]; i++) {
            uint32_t v = g -> out_dest[i];  
            T weight = g->out_weight[i];

            if (dis[v] > dis[u] + weight) {
                dis[v] = dis[u] + weight; 
                q.push(v); 
            }
        }
    }

    VLOG(1) << "correct top 100: " << log_array<T>(dis, uint64_t(100)).str();
    return dis;
}

uint32_t *cc(raw_graph<empty> *g, uint32_t total_v) { // only undirected graph
    uint32_t *cc_id = new uint32_t[total_v];
    for (uint32_t i = 0; i < total_v; i++) {
        cc_id[i] = i;
    }
    bool changed = true;
    while (changed) {
        changed = false;
        for (uint32_t u = 0; u < total_v; u++) {
            for (uint32_t i = g->out_offset[u]; i < g->out_offset[u + 1]; i++) {
                uint32_t v = g->out_dest[i];
                if (u < v) {
                    if (cc_id[v] > cc_id[u]) {
                        cc_id[v] = cc_id[u];
                        changed = true;
                    } else if (cc_id[u] > cc_id[v]) {
                        cc_id[u] = cc_id[v];
                        changed = true;
                    }
                }
            }
        }
    }
    VLOG(1) << "correct top 100: " << log_array<uint32_t>(cc_id, uint64_t(100)).str();
    return cc_id;
}

template <class T>
T *read_result_from_file(std::string result_dir, uint32_t total_v) {
    T *result = new T[total_v];
    for (const auto &entry : fs::directory_iterator(result_dir)) {
        check_result_freshness(entry.path());
        read_result_util<T>(entry.path(), result);
    }
    VLOG(1) << "result from local file top 100: " << log_array<T>(result, uint64_t(100)).str();
    return result;
}

template <class T>
T *read_result_from_s3(uint32_t total_v) {
    T *result = new T[total_v];
    std::vector<std::string> objects = s_sdk -> list_objects(FLAGS_s3_bucket);
    for (auto object : objects) {
        s_sdk -> check_object_freshness(FLAGS_s3_bucket, object);
        std::pair<char *, uint32_t> raw_data = s_sdk -> get_object(FLAGS_s3_bucket, object);
        T *data = (T *)raw_data.first;
        uint32_t len = raw_data.second >> 2;
        uint32_t start = get_start_from_result_file_name(object);
        for (uint32_t i = 0; i < len; i++) {
            result[start + i] = data[i];
        }
    }
    VLOG(1) << "result from s3 top 100: " << log_array<T>(result, uint64_t(100)).str();
    return result;
}

template <class T>
T *read_result(CAAS_SAVE_MODE save_mode, std::string result_dir, uint32_t total_v) {
    switch (save_mode) {
        case CAAS_SAVE_MODE::NO_SAVE:
            break;
        case CAAS_SAVE_MODE::SAVE_LOCAL:
            return read_result_from_file<T>(result_dir, total_v);
        case CAAS_SAVE_MODE::SAVE_S3:
            return read_result_from_s3<T>(total_v);
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

template <typename T>
void process_sssp(CAAS_SAVE_MODE save_mode) {
    raw_graph<T> g(FLAGS_vertices, FLAGS_edges * (FLAGS_undirected ? 2 : 1));
    g.read_txt(FLAGS_graph_file, FLAGS_undirected);

    T* dis1 = sssp<T>(&g, FLAGS_vertices, FLAGS_sssp_root);
    T* dis2 = read_result<T>(save_mode, FLAGS_result_dir, FLAGS_vertices);
    check_equal<T>(FLAGS_vertices, dis1, dis2);

    delete[] dis1;
    delete[] dis2;
}


int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    exec_config *config = exec_config::build_by_flags();
    VLOG(1) << "aws sdk init";
    if (config -> enable_sdk()) {
        sdk_init();
    }
    if (config -> enable_s3_sdk()) {
        s_sdk = new s3_sdk();
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
    } else if (FLAGS_application == "sssp"){
        if (FLAGS_ewT == "uint32_t") {
            process_sssp<uint32_t>((CAAS_SAVE_MODE)FLAGS_save_mode);
        } else if (FLAGS_ewT == "int") {
            process_sssp<int>((CAAS_SAVE_MODE)FLAGS_save_mode);
        } else if (FLAGS_ewT == "float") {
            process_sssp<float>((CAAS_SAVE_MODE)FLAGS_save_mode);
        } else {
            VLOG(1) << "Unsupported edge weight type: " << FLAGS_ewT;
        }
    } else if (FLAGS_application == "cc") {
        // only undirected graph
        raw_graph<empty> g(FLAGS_vertices, FLAGS_edges * 2);
        g.read_txt(FLAGS_graph_file, true);
        uint32_t *cc1 = cc(&g, FLAGS_vertices); 
        uint32_t *cc2 = read_result<uint32_t>((CAAS_SAVE_MODE)FLAGS_save_mode, FLAGS_result_dir, FLAGS_vertices);
        check_equal<uint32_t>(FLAGS_vertices, cc1, cc2);
    }
    VLOG(1) << "Freshness Check (<=30min) Passed";   
    VLOG(1) << "Correctness Check Passed";
    return 0;
}