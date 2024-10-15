#include "preprocess/raw_graph.h"
#include "preprocess/graph_set.h"
#include "preprocess/partition.h"
#include "util/types.h"
#include "util/log.h"

#include <cstring>

int main(int argc, char *argv[]) {

    google::InitGoogleLogging(argv[0]);
    google::SetLogDestination(google::INFO, "log.txt");

    raw_graph<empty> g(32, 31);
    g.read_txt("/home/ubuntu/FaaSBoard/example_data/example_graph.txt");
    g.print();

    partition_result result;
    std::vector<graph_set<empty> *> graphsets;
    int total_block = 4, cut;

    LOG(INFO) << "optimal cut + binpack";
    cut = sqrt((double)total_block) + 1;
    result = g.checkerboard_partition(cut);
    result.print();
    double unbalance_ratio = result.get_unbalance_ratio();
    LOG(INFO) << "unbalance ratio: " << unbalance_ratio;
    LOG(INFO) << "begin partitioning";
    graphsets = g.partition(result);
    for (auto graphset : graphsets) {
        graphset -> print(true);
    }
    try {
        double balance_ratio;
        std::cout << "balance ratio?" << std::endl;
        std::cin >> balance_ratio;
        std::vector<graph_set<empty> *> new_graphsets = graph_set<empty>::binpack(graphsets, total_block, balance_ratio);
        for (auto graphset : new_graphsets) {
            graphset -> print(false);
        }
        graph_set<empty>::simulation(new_graphsets);
        graph_set<empty>::save(new_graphsets, "/home/ubuntu/FaaSBoard/data");
    } catch (const std::runtime_error &e) {
        std::cout << e.what() << std::endl;
    }

    return 0;
}