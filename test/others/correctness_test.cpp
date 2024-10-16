#include "preprocess/raw_graph.h"
#include "util/log.h"
#include "util/flags.h"

#include <cstring>
#include <queue>
#include <filesystem>

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

template <class T>
T *read_result(std::string graph_root_dir, uint32_t total_v) {
    fs::path root_path(graph_root_dir);
    T *result = new T[total_v];
    int dir_index = 0;
    for (;;) {
        if (!fs::exists(root_path / std::to_string(dir_index))) {
            break;
        }
        int file_index = 0;
        for (;;) {
            fs::path result_file_path = root_path / std::to_string(dir_index) / ("result" + std::to_string(file_index) + ".txt");
            if (!fs::exists(result_file_path)) {
                break;
            }
            std::ifstream result_file(result_file_path);
            std::string line;
            while (std::getline(result_file, line)) {
                std::istringstream iss(line);
                uint32_t a;
                T b;
                if (iss >> a >> b) {
                    result[a] = b;
                } else {
                    LOG(FATAL) << "error parsing line " << line;
                }
            }
            file_index++;
        }   
        dir_index++;
    }
    VLOG(1) << "result top 100: " << log_array<uint32_t>(result, 100).str();
    return result;
}

template <class T>
void check(uint32_t total_v, T *vec1, T *vec2) {
    for (uint32_t i = 0; i < total_v; i++) {
        CHECK(vec1[i] == vec2[i]) << "vertex " << i << " correct " << vec1[i] << " result " << vec2[i];
    }
    VLOG(1) << "Correctness Check Passed";
}

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;
    raw_graph<empty> g(FLAGS_vertices, FLAGS_edges);
    g.read_txt(FLAGS_graph_file);
    if (FLAGS_application == "bfs") {
        uint32_t *dis1 = bfs(&g, FLAGS_vertices, FLAGS_bfs_root);
        uint32_t *dis2 = read_result<uint32_t>(FLAGS_graph_root_dir, FLAGS_vertices);
        check<uint32_t>(FLAGS_vertices, dis1, dis2);
    }   
    return 0;
}