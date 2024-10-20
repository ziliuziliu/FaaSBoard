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

#define MAX_CONNECTION 4096

class segment_base {
    
public:

    uint32_t *data;
    bitmap *bm;
    std::mutex m;
    int root_fd;
    std::vector<int> fds;
    int cnt, bitmap_len, vec_len;
    uint32_t base_vertex_value, flag;
    bool initialized;

    segment_base() {}

    segment_base(int vec_len, uint32_t base_vertex_value, uint32_t flag, bool has_bitmap) {
        this -> root_fd = -1;
        this -> fds = std::vector<int>();
        this -> flag = flag;
        this -> cnt = 0;
        this -> bitmap_len = has_bitmap ? bitmap::get_bitmap_length_bits(vec_len) >> 5 : 0;
        this -> vec_len = vec_len;
        this -> data = new uint32_t[5 + bitmap_len + vec_len];
        this -> bm = has_bitmap ? new bitmap(vec_len, data + 5) : nullptr;
        this -> base_vertex_value = base_vertex_value;
        this -> initialized = false;
        reset();
    }

    void reset() {
        cnt = 0;
        if (bm != nullptr) {
            bm -> clear();
        }
        for (int i = 0; i < vec_len; i++) {
            data[5 + bitmap_len + i] = base_vertex_value;
        }
    }

    bool all_connected() {
        return caas_flag_get_members(flag) == fds.size() + (root_fd != -1);
    }

    bool adaptive_segment() {
        return bm -> get_size() <= CAAS_SPARSE_LIMIT ? CAAS_SPARSE : CAAS_DENSE;
    }

    void initialize(uint32_t *header) {
        memcpy(data, header, 20);
        initialized = true;
    }

    std::pair<char *, size_t> make_adaptive_segment(bool segment_type) {
        caas_segment_set_segment_type(data + 4, segment_type);
        if (segment_type == CAAS_DENSE) {
            return {(char *)data, (5 + bitmap_len + vec_len) << 2};
        } else {
            uint32_t segment_len = 5 + bitmap_len + bm -> get_size();
            uint32_t *segment = new uint32_t[segment_len];
            uint32_t pos = 5 + bitmap_len;
            memcpy(segment, data, 20 + (bitmap_len << 2));
            for (uint32_t i = 0; i < (uint32_t)vec_len; i++) {
                if (bm -> exist(i)) {
                    segment[pos] = data[5 + bitmap_len + i];
                    pos++;
                }
            }
            return {(char *)segment, segment_len << 2};
        }
    }

    void reduce_adaptive_segment(char *raw_data, size_t len) {
        uint32_t *segment = (uint32_t *)raw_data;
        uint32_t bitmap_len = segment[2], vec_len = segment[3], flag = segment[4];
        bool segment_type = caas_segment_get_segment_type(flag);
        uint8_t data_type = caas_flag_get_data_type(flag);
        uint8_t reduce_op = caas_flag_get_reduce_op(flag);
        if (segment_type == CAAS_DENSE) {
            reduce_vec_masked_dense(data + 5 + bitmap_len, segment + 5 + bitmap_len, vec_len, bm, reduce_op, data_type);
        } else {
            bitmap *segment_bm = new bitmap(vec_len, segment + 5);
            reduce_vec_masked_sparse(data + 5 + bitmap_len, segment + 5 + bitmap_len, vec_len, bm, segment_bm, reduce_op, data_type);
        }
    }

    std::pair<char *, size_t> make_segment() {
        return {(char *)data, (5 + vec_len) << 2};
    }

    void reduce_segment(char *raw_data, size_t len) {
        uint32_t *segment = (uint32_t *)raw_data;
        uint32_t vec_len = segment[3], flag = segment[4];
        uint8_t data_type = caas_flag_get_data_type(flag);
        uint8_t reduce_op = caas_flag_get_reduce_op(flag);
        reduce_vec(data + 5, segment + 5, vec_len, reduce_op, data_type);
    }

};

moodycamel::BlockingReaderWriterCircularBuffer<int> **fd_queue;
std::unordered_map<int, int> fd_flag;
std::unordered_map<uint32_t, std::unordered_map<uint32_t, segment_base *>> segment_table;
std::unordered_map<int, segment_base *> fd_segment;

void work(int thread_id) {
    while (true) {
        int client_fd;
        fd_queue[thread_id] -> wait_dequeue(client_fd);
        std::pair<char *, size_t> raw_data = caas_recv_all(client_fd);
        if (raw_data.first == nullptr) {
            close(client_fd);
            cas<int>(&fd_flag[client_fd], CAAS_FD_INQUEUE, CAAS_FD_NOTINQUEUE);
            VLOG(1) << "fd " << client_fd << " disconnected";
            continue;
        }
        uint32_t *data = (uint32_t *)raw_data.first;
        uint32_t request_id = data[0], object_id = data[1], flag = data[4];
        segment_base *segment;
        segment = segment_table[request_id][object_id];
        timer t;
        switch (caas_flag_get_comm_op(flag)) {
            case CAAS_MASKED_BROADCAST:
                VLOG(1) << "masked broadcast from request " << request_id 
                    << " object " << object_id
                    << " fd " << client_fd;
                #pragma omp parallel for
                for (int i = 0; i < (int)segment -> fds.size(); i++) {
                    caas_send_all(segment -> fds[i], raw_data.first, raw_data.second);
                }
                delete [] raw_data.first;
                break;
            case CAAS_MASKED_REDUCE:
                VLOG(1) << "masked reduce from request " << request_id 
                    << " object " << object_id
                    << " fd " << client_fd;
                t.tick("lock_wait");
                segment -> m.lock();
                t.from_tick();
                if (!segment -> initialized) {
                    segment -> initialize((uint32_t *)raw_data.first);
                }
                t.tick("reduce_adaptive_segment");
                segment -> reduce_adaptive_segment(raw_data.first, raw_data.second);
                segment -> cnt++;
                t.from_tick();
                if (segment -> cnt == (int)segment -> fds.size()) {
                    bool segment_type = segment -> adaptive_segment();
                    t.tick("make_adaptive_segment");
                    std::pair<char *, size_t> new_data = segment -> make_adaptive_segment(segment_type);
                    t.from_tick();
                    t.tick("send_all");
                    caas_send_all(segment -> root_fd, new_data.first, new_data.second);
                    t.from_tick();
                    if (segment_type == CAAS_SPARSE) {
                        delete [] new_data.first;
                    }
                    segment -> reset();
                }
                segment -> m.unlock();
                break;
            case CAAS_ALLREDUCE:
                VLOG(1) << "all reduce from request " << request_id 
                    << " object " << object_id
                    << " fd " << client_fd;
                segment -> m.lock();
                if (!segment -> initialized) {
                    segment -> initialize((uint32_t *)raw_data.first);
                }
                segment -> reduce_segment(raw_data.first, raw_data.second);
                segment -> cnt++;
                if (segment -> cnt == (int)segment -> fds.size()) {
                    std::pair<char *, size_t> new_data = segment -> make_segment();
                    #pragma omp parallel for
                    for (int i = 0; i < (int)segment -> fds.size(); i++) {
                        caas_send_all(segment -> fds[i], new_data.first, new_data.second);
                    }
                    segment -> reset();
                }
                segment -> m.unlock();
                break;
            default:
                LOG(FATAL) << "undefined comm op " << caas_flag_get_comm_op(flag);
        }
        cas<int>(&fd_flag[client_fd], CAAS_FD_INQUEUE, CAAS_FD_NOTINQUEUE);
    }
}

void run() {
    VLOG(1) << "proxy running on " << FLAGS_cores << " cores";
    fd_queue = new moodycamel::BlockingReaderWriterCircularBuffer<int>*[FLAGS_cores];
    for (int i = 0; i < (int)FLAGS_cores; i++) {
        fd_queue[i] = new moodycamel::BlockingReaderWriterCircularBuffer<int>(MAX_CONNECTION);
        std::thread worker(work, i);
        worker.detach();
    }
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(20001);
    int status_code = bind(server_fd, (sockaddr *)&server_address, sizeof(sockaddr_in));
    CHECK(status_code == 0) << "cannot bind to 20001";
    listen(server_fd, MAX_CONNECTION);
    VLOG(1) << "proxy server started";
    int epoll_fd = epoll_create1(0);
    epoll_event event, events[MAX_CONNECTION];
    event.events = EPOLLIN;
    event.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event);
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
                fd_flag[client_fd] = CAAS_FD_NOTINQUEUE;
                uint32_t connect_data[6];
                recv(client_fd, connect_data, sizeof(uint32_t) * 6, 0);
                uint32_t request_id = connect_data[0], object_id = connect_data[1];
                uint32_t vec_len = connect_data[2], base_vertex_value = connect_data[3];
                uint32_t flag = connect_data[4];
                bool has_bitmap = connect_data[5];
                VLOG(1) << "connection from request " << request_id 
                    << " object " << object_id 
                    << " vec_len " << vec_len
                    << " assigned fd " << client_fd;
                if (segment_table.count(request_id) == 0) {
                    segment_table[request_id] = std::unordered_map<uint32_t, segment_base *>();
                }
                segment_base *segment;
                if (segment_table[request_id].count(object_id) == 0) {
                    segment_table[request_id][object_id] = new segment_base(vec_len, base_vertex_value, flag, has_bitmap);
                }
                segment = segment_table[request_id][object_id];
                fd_segment[client_fd] = segment;
                if (caas_flag_get_root(flag)) {
                    segment -> root_fd = client_fd;
                } else {
                    segment -> fds.push_back(client_fd);
                }
            } else if (events[i].events & EPOLLHUP) {
                VLOG(1) << "fd " << events[i].data.fd << " disconnected";
            } else if (events[i].events & EPOLLERR) {
                LOG(FATAL) << "fd " << events[i].data.fd << " error";
            } else {
                int client_fd = events[i].data.fd;
                if (!fd_segment[client_fd] -> all_connected()) {
                    continue;
                }
                if (!cas<int>(&fd_flag[client_fd], CAAS_FD_NOTINQUEUE, CAAS_FD_INQUEUE)) {
                    continue;
                }
                int mn_size = 0x7fffffff, mn_thread_id = -1;
                for (int i = 0; i < (int)FLAGS_cores; i++) {
                    int current_size = fd_queue[i] -> size_approx();
                    if (current_size < mn_size) {
                        mn_size = current_size;
                        mn_thread_id = i;
                    }
                }
                fd_queue[mn_thread_id] -> wait_enqueue(client_fd);
            }
        }
    }
}

#endif