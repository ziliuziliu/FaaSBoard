#ifndef _PROXY_SERVER_H
#define _PROXY_SERVER_H

#include "caas.h"
#include "util/atomic.h"
#include "util/log.h"
#include "util/flags.h"
#include "util/bitmap.h"
#include "util/spsc/readerwritercircularbuffer.h"
#include "util/types.h"
#include "util/timer.h"
#include "util/sdk.h"

#include <sys/socket.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <omp.h>
#include <string>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <chrono>
#include <immintrin.h>
#include <unordered_set>
#include <condition_variable>

#define MAX_CONNECTION 4096

uint64_t combine(uint32_t a, uint32_t b) {
    return (uint64_t)a << 32 | b;
}

struct binomial_node {
    uint32_t object_id; // represent the index of the tree
    uint32_t tree_id; // represent the index of the node on the tree
    bool operator==(const binomial_node& other) const {
        return object_id == other.object_id && tree_id == other.tree_id;
    }
};

struct binomial_node_hash {
    std::size_t operator()(const binomial_node& node) const {
        return std::hash<uint32_t>()(node.object_id) ^ (std::hash<uint32_t>()(node.tree_id) << 1);
    }
};

class BinomialNodeMap {
    private:
        std::unordered_map<binomial_node, int, binomial_node_hash> fd_map;
    
    public:
        // Print all elements in the map
        void print_all() const {
            VLOG(1) << "BinomialNodeMap contents:";
            for (const auto& pair : fd_map) {
                VLOG(1) << "Node: (object_id=" << pair.first.object_id
                      << ", tree_id=" << pair.first.tree_id
                      << "), FD: " << pair.second;
            }
        }


        // bind fd to (object_id, tree_id)
        void bind_fd(uint32_t object_id, uint32_t tree_id, int fd) {
            binomial_node node = {object_id, tree_id};
            fd_map[node] = fd;
        }
    
        // get fd by (object_id, tree_id)
        int get_fd(uint32_t object_id, uint32_t tree_id) const {
            binomial_node node = {object_id, tree_id};
            auto it = fd_map.find(node);
            if (it != fd_map.end()) {
                return it -> second;
            } else {
                return -1;
            }
        }
    
        void unbind_fd(uint32_t object_id, uint32_t tree_id) {
            binomial_node node = {object_id, tree_id};
            fd_map.erase(node);
        }

        // find and remove node by fd
        void find_and_remove_by_fd(int fd) {
            for (auto it = fd_map.begin(); it != fd_map.end(); ) {
                if (it -> second == fd) {
                    it = fd_map.erase(it);
                    return; 
                } else {
                    ++it;
                }
            }
            return;
        }
    
        void clear() {
            fd_map.clear();
        }
};


class segment_base {
    
public:

    uint32_t request_id, object_id;
    uint32_t *data;
    bitmap *bm;
    std::mutex m;
    int root_fd;
    std::unordered_set<int> fds;
    int round, reduce_cnt, bitmap_len, vec_len;
    uint32_t base_vertex_value, flag;
    uint8_t instances;
    __m256i base_vertex_value_m256;
    bool initialized;

    segment_base() {}

    segment_base(uint32_t request_id, uint32_t object_id, int vec_len, uint32_t base_vertex_value, uint32_t flag, bool has_bitmap) {
        this -> request_id = request_id;
        this -> object_id = object_id;
        this -> root_fd = -1;
        this -> fds = std::unordered_set<int>();
        this -> flag = flag;
        this -> instances = caas_flag_get_instances(flag);
        this -> round = 0;
        this -> reduce_cnt = 0;
        this -> bitmap_len = has_bitmap ? bitmap::get_bitmap_length_bits(vec_len) >> 5 : 0;
        this -> vec_len = vec_len;
        this -> data = new uint32_t[7 + bitmap_len + vec_len];
        this -> bm = has_bitmap ? new bitmap(vec_len, data + 7) : nullptr;
        this -> base_vertex_value = base_vertex_value;
        this -> base_vertex_value_m256 = _mm256_set1_epi32(base_vertex_value);
        this -> initialized = false;
        reset();
    }

    ~segment_base() {
        delete [] data;
    }

    void reset() {
        reduce_cnt = 0;
        if (bm != nullptr) {
            bm -> clear();
        }
        int i;
        for (i = 0; i + 8 < vec_len; i += 8) {
            _mm256_storeu_si256((__m256i*)&data[7 + bitmap_len + i], base_vertex_value_m256);
        }
        for (; i < vec_len; i++) {
            data[7 + bitmap_len + i] = base_vertex_value;
        }
    }

    bool all_connected() {
        VLOG(1) << "all_connected:: intances = " << int(instances) << "; fds.size() = " << fds.size() << "; root_fd = " << root_fd; 
        return instances == fds.size() + (root_fd != -1);
    }

    bool none_connected() {
        return fds.size() == 0 && root_fd == -1;
    }

    void initialize(uint32_t *header) {
        memcpy(data, header, 28);
        initialized = true;
    }

    std::pair<char *, size_t> make_segment() {
        return {(char *)data, (7 + vec_len) << 2};
    }

    void reduce_segment(char *raw_data, size_t len) {
        uint32_t *segment = (uint32_t *)raw_data;
        uint32_t vec_len = segment[3], flag = segment[4];
        CAAS_TYPE data_type = caas_flag_get_data_type(flag);
        CAAS_REDUCE_OP reduce_op = caas_flag_get_reduce_op(flag);
        reduce_vec(data + 7, segment + 7, vec_len, reduce_op, data_type);
    }

};

moodycamel::BlockingReaderWriterCircularBuffer<int> **fd_queue;
std::unordered_map<int, int> fd_in_queue;
std::unordered_map<int, int> thread_stuck;
std::unordered_map<int, segment_base *> fd_segment;
std::mutex fd_segment_m;
std::unordered_map<uint64_t, segment_base *> segment_table;
std::mutex segment_table_m;
std::unordered_map<uint32_t, REQUEST_EXECUTION_STATUS> request_status_table;
std::mutex request_status_table_m;
exec_config *config;
BinomialNodeMap node_map;

void add_fd_to_segment(int fd, bool root, segment_base *segment) {
    if (root) {
        if (segment -> root_fd != -1) {
            LOG(FATAL) << "try to add root fd " << fd << " but already have " << segment -> root_fd;
        }
        segment -> root_fd = fd;
    } else {
        if (segment -> fds.contains(fd)) {
            LOG(FATAL) << "already have " << fd << " in fds";
        }
        segment -> fds.insert(fd);
    }
    VLOG(1) << "object id " << segment -> object_id
        << " add fd " << fd << " root " << (int)root 
        << " current fds " << (int)segment -> fds.size()
        << " all connected " << segment -> all_connected();
}

void remove_fd_from_segment(int fd, segment_base *segment) {
    if (segment -> root_fd == fd) {
        segment -> root_fd = -1;
    } else {
        if (!segment -> fds.contains(fd)) {
            LOG(FATAL) << "don't have " << fd << " in fds";
        }
        segment -> fds.erase(fd);
    }
    VLOG(1) << "object id " << segment -> object_id
        << " remove fd " << fd 
        << " current fds " << (int)segment -> fds.size();
}

void work(int thread_id, int epoll_fd) {
    while (true) {
        int client_fd;
        fd_queue[thread_id] -> wait_dequeue(client_fd);
        cas<int>(&thread_stuck[thread_id], 0, 1);
        std::pair<char *, size_t> raw_data = caas_recv_all(client_fd);
        cas<int>(&thread_stuck[thread_id], 1, 0);
        if (raw_data.second == 1) {
            VLOG(1) << "error msg " << (int)raw_data.first[0];
        }
        segment_base *segment;
        {
            std::lock_guard<std::mutex> fd_segment_lg(fd_segment_m);
            segment = fd_segment[client_fd];
        }
        if (raw_data.first == nullptr) {
            VLOG(1) << "fd " << client_fd << " closed";
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, NULL);
            close(client_fd);

            {
                std::lock_guard<std::mutex> fd_segment_lg(fd_segment_m);
                fd_segment.erase(client_fd);
                node_map.find_and_remove_by_fd(client_fd);
            }
            segment -> m.lock();
            remove_fd_from_segment(client_fd, segment);
            segment -> m.unlock();

            if (segment -> none_connected()) {
                {
                    std::lock_guard<std::mutex> request_status_table_lg(request_status_table_m);
                    request_status_table[segment -> request_id] = REQUEST_EXECUTION_STATUS::INIT;
                }
                {
                    std::lock_guard<std::mutex> segment_table_lg(segment_table_m);
                    segment_table.erase(combine(segment -> request_id, segment -> object_id));
                }
                VLOG(1) << "request " << segment -> request_id << " object " << segment -> object_id << " deleted";
                delete segment;
            }
            cas<int>(&fd_in_queue[client_fd], 1, 0);
            continue;
        }

        uint32_t *data = (uint32_t *)raw_data.first;
        uint32_t request_id = data[0], object_id = data[1], flag = data[4], from_id = data[5], to_id = data[6];
        // VLOG(1) << "raw_data:: " 
        //             << "request_id = " << data[0]
        //             << "; object_id = " << data[1]
        //             << "; bitmap_len = " << data[2]
        //             << "; vec_len = " << data[3]
        //             << "; reduce_op = " << (int)caas_flag_get_reduce_op(data[4])
        //             << "; comm_op/caas_op = " << (int)caas_flag_get_comm_op(data[4])
        //             << "; from_id = " << data[5]
        //             << "; to_id = " << data[6]
        //             << "; client_fd = " << client_fd;

        switch (caas_flag_get_comm_op(flag)) {
            case CAAS_OP::MASKED_BROADCAST: {
                int to_fd  = node_map.get_fd(object_id, to_id);
                // VLOG(1) << "masked_broadcast::trans from request " << request_id 
                //             << " object " << object_id
                //             << " from_id " << from_id
                //             << " to_id " << to_id
                //             << " client_fd " << client_fd;
                caas_send_all(to_fd, raw_data.first, raw_data.second);
                delete [] raw_data.first;
                break;
            }

            case CAAS_OP::MASKED_REDUCE: {
                int to_fd  = node_map.get_fd(object_id, to_id);
                // VLOG(1) << "masked_reduce::trans from request " << request_id 
                //             << " object " << object_id
                //             << " from_id " << from_id
                //             << " to_id " << to_id
                //             << " client_fd " << client_fd;
                VLOG(1) << "masked_reduce:: " 
                    << "request_id = " << data[0]
                    << "; object_id = " << data[1]
                    << "; bitmap_len = " << data[2]
                    << "; vec_len = " << data[3]
                    << "; reduce_op = " << (int)caas_flag_get_reduce_op(data[4])
                    << "; comm_op/caas_op = " << (int)caas_flag_get_comm_op(data[4])
                    << "; from_id = " << data[5]
                    << "; to_id = " << data[6]
                    << "; client_fd = " << client_fd
                    << "; to_fd = " << to_fd
                    << "send:: len = " << raw_data.second;
                caas_send_all(to_fd, raw_data.first, raw_data.second);
                delete [] raw_data.first;
                break;
            }

            case CAAS_OP::ALLREDUCE:
                // VLOG(1) << "all reduce from request " << request_id 
                //             << " object " << object_id
                //             << " from_id " << from_id
                //             << " to_id " << to_id
                //             << " client_fd " << client_fd;
                VLOG(1) << "all_reduce:: " 
                    << "request_id = " << data[0]
                    << "; object_id = " << data[1]
                    << "; bitmap_len = " << data[2]
                    << "; vec_len = " << data[3]
                    << "; reduce_op = " << (int)caas_flag_get_reduce_op(data[4])
                    << "; comm_op/caas_op = " << (int)caas_flag_get_comm_op(data[4])
                    << "; from_id = " << data[5]
                    << "; to_id = " << data[6]
                    << "; client_fd = " << client_fd
                    // << "; to_fd = " << to_fd;
                    << "all_len = " << raw_data.second;
                segment -> m.lock();
                if (!segment -> initialized) {
                    segment -> initialize((uint32_t *)raw_data.first);
                }
                segment -> reduce_segment(raw_data.first, raw_data.second);
                segment -> reduce_cnt++;
                if (segment -> reduce_cnt == (int)segment -> instances) {
                    segment -> round++;
                    VLOG(1) << "now we have enough instances, reduce_cnt " << segment -> reduce_cnt;
                    if (!segment -> all_connected()) {
                        LOG(FATAL) << "request " << request_id << " object " << object_id << " not all connected";
                    }
                    {
                        std::lock_guard<std::mutex> request_status_table_lg(request_status_table_m);
                        request_status_table[segment -> request_id] = REQUEST_EXECUTION_STATUS::EXECUTE;
                    }
                    std::pair<char *, size_t> new_data = segment -> make_segment();
                    std::vector<int> fd_list;
                    fd_list = std::vector<int>(segment -> fds.begin(), segment -> fds.end());
                    #pragma omp parallel for
                    for (int i = 0; i < (int)fd_list.size(); i++) {
                        VLOG(1) << "all_reduce:: send to_fd = " << fd_list[i] << "; len = " << new_data.second;
                        caas_send_all(fd_list[i], new_data.first, new_data.second);
                    }
                    segment -> reset();
                }
                segment -> m.unlock();
                break;

            default:
                LOG(FATAL) << "undefined comm op " << (int)caas_flag_get_comm_op(flag);
        }
        cas<int>(&fd_in_queue[client_fd], 1, 0);
    }
}

void run() {
    config = exec_config::build_by_flags();
    VLOG(1) << "aws sdk init";
    if (config -> enable_sdk()) {
        sdk_init();
    }
    VLOG(1) << "proxy running on " << config -> cores << " cores";

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(20001);
    int status_code = bind(server_fd, (sockaddr *)&server_address, sizeof(sockaddr_in));
    CHECK(status_code == 0) << "cannot bind to 20001";
    listen(server_fd, MAX_CONNECTION);
    int epoll_fd = epoll_create1(0);
    epoll_event event, events[MAX_CONNECTION];
    event.events = EPOLLIN;
    event.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event);
    VLOG(1) << "proxy server started";

    fd_queue = new moodycamel::BlockingReaderWriterCircularBuffer<int>*[config -> cores];
    for (int i = 0; i < (int)config -> cores; i++) {
        fd_queue[i] = new moodycamel::BlockingReaderWriterCircularBuffer<int>(MAX_CONNECTION);
        std::thread worker(work, i, epoll_fd);
        worker.detach();
    }
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        int num_events = epoll_wait(epoll_fd, events, MAX_CONNECTION, -1);
        for (int i = 0; i < num_events; i++) {
            if (events[i].data.fd == server_fd) {
                sockaddr_in client_address;
                socklen_t client_address_size = sizeof(sockaddr_in);
                int client_fd = accept(server_fd, (sockaddr *)&client_address, &client_address_size);
                event.events = EPOLLIN;
                event.data.fd = client_fd;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event);
                fd_in_queue[client_fd] = 0;
                uint32_t connect_data[8];
                recv(client_fd, connect_data, sizeof(uint32_t) * 8, 0);
                uint32_t request_id = connect_data[0], partition_id = connect_data[1], object_id = connect_data[2], tree_id = connect_data[3];
                uint32_t vec_len = connect_data[4], base_vertex_value = connect_data[5];
                uint32_t flag = connect_data[6];
                if (vec_len == 1) {
                    VLOG(1) << "vec_len = 1 :: root = " << (int)caas_flag_get_root(flag);
                }
                bool has_bitmap = connect_data[7];
                VLOG(1) << "connection from request " << request_id 
                    << " partition " << partition_id
                    << " object " << object_id 
                    << " tree_id " << tree_id
                    << " vec_len " << vec_len
                    << " assigned fd " << client_fd;
                
                REQUEST_EXECUTION_STATUS status;
                {
                    std::lock_guard<std::mutex> request_status_table_lg(request_status_table_m);
                    if (request_status_table.count(request_id) == 0) {
                        request_status_table[request_id] = REQUEST_EXECUTION_STATUS::INIT;
                    }
                    status = request_status_table[request_id];
                }
                if (status == REQUEST_EXECUTION_STATUS::EXECUTE) {
                    VLOG(1) << "wrong connection, closing fd " << client_fd;
                    close(client_fd);
                    continue;
                }

                segment_base *segment;
                {
                    std::lock_guard<std::mutex> segment_table_lg(segment_table_m);
                    uint64_t roid = combine(request_id, object_id);
                    if (segment_table.count(roid) == 0) {
                        segment_table[roid] = new segment_base(request_id, object_id, vec_len, base_vertex_value, flag, has_bitmap);
                    }
                    segment = segment_table[roid];
                }
                segment -> m.lock();
                add_fd_to_segment(client_fd, caas_flag_get_root(flag), segment);
                segment -> m.unlock();
                {
                    std::lock_guard<std::mutex> fd_segment_lg(fd_segment_m);
                    fd_segment[client_fd] = segment;
                }
                node_map.bind_fd(object_id, tree_id, client_fd);  
            } else if (events[i].events & (EPOLLRDHUP | EPOLLHUP)) {
                LOG(FATAL) << "fd " << events[i].data.fd << " epollhup";
            } else if (events[i].events & EPOLLERR) {
                LOG(FATAL) << "fd " << events[i].data.fd << " epollerr";
            } else {
                int client_fd = events[i].data.fd;
                if (!cas<int>(&fd_in_queue[client_fd], 0, 1)) {
                    continue;
                }
                int mn_size = 0x7fffffff, mn_thread_id = -1;
                for (int i = 0; i < (int)config -> cores; i++) {
                    if (thread_stuck[i]) {
                        continue;
                    }
                    int current_size = fd_queue[i] -> size_approx();
                    if (current_size < mn_size) {
                        mn_size = current_size;
                        mn_thread_id = i;
                    }
                }
                if (mn_thread_id == -1) {
                    LOG(FATAL) << "no available thread";
                }
                // VLOG(1) << "enqueue fd " << client_fd << " to thread " << mn_thread_id;
                fd_queue[mn_thread_id] -> wait_enqueue(client_fd);
            }
        }
    }
}

#endif