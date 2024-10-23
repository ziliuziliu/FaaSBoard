#ifndef _CAAS_H
#define _CAAS_H

#include "util/bitmap.h"
#include "util/types.h"
#include "util/reduce.h"
#include "util/log.h"
#include "util/timer.h"

#include <vector>
#include <arpa/inet.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

template <class T>
uint8_t caas_get_data_type() {
    if (std::is_same<int, T>::value) {
        return CAAS_INT;
    } else if (std::is_same<float, T>::value) {
        return CAAS_FLOAT;
    }
    return CAAS_UINT32;
}

uint32_t caas_build_flag(bool root, uint8_t members, uint8_t data_type, uint8_t comm_op, uint8_t reduce_op) {
    uint32_t flag = 0;
    flag |= (root << 31);
    flag |= ((members & 0xf) << 12);
    flag |= ((data_type & 0xf) << 8);
    flag |= ((comm_op & 0xf) << 4);
    flag |= (reduce_op & 0xf);
    return flag;
}

bool caas_flag_get_root(uint32_t flag) {
    return flag >> 31;
}

uint8_t caas_flag_get_members(uint32_t flag) {
    return (flag >> 12) & 0xf;
}

uint8_t caas_flag_get_data_type(uint32_t flag) {
    return (flag >> 8) & 0xf;
}

uint8_t caas_flag_get_comm_op(uint32_t flag) {
    return (flag >> 4) & 0xf;
}

uint8_t caas_flag_get_reduce_op(uint32_t flag) {
    return flag & 0xf;
}

bool caas_segment_get_segment_type(uint32_t flag) {
    return flag & (1 << 30);
}

void caas_segment_set_segment_type(uint32_t *flag, bool segment_type) {
    *flag = ((*flag) & ~(1 << 30)) | (segment_type << 30);
}

void caas_send_all(int fd, char *data, size_t len) {
    size_t pos = 0;
    send(fd, &len, sizeof(size_t), 0);
    while (pos < len) {
        ssize_t send_size = send(fd, data + pos, len - pos, 0);
        pos += (size_t)send_size;
    }
}

std::pair<char *, size_t> caas_recv_all(int fd) {
    size_t len, pos = 0;
    ssize_t signal = recv(fd, &len, sizeof(size_t), 0);
    if (signal == 0) {
        return {nullptr, 0};
    }
    char *data = new char[len];
    while (pos < len) {
        ssize_t recv_size = recv(fd, data + pos, len - pos, 0);
        pos += (size_t)recv_size;
    }
    return {data, len};
}

template <class T>
class comm_object {
    
public:
    uint32_t request_id;
    uint32_t object_id;
    uint32_t bitmap_len;
    uint32_t vec_len;
    // | root, 1 bit | segment_type, 1 bit | 14 bit | members 4 bit | data_type, 4 bit | comm_op, 4 bit | reduce_op, 4 bit |
    uint32_t flag;

    uint32_t *data;

    uint32_t start_index;
    bitmap *bm;
    T *vec;

    bool has_bitmap, root;
    uint8_t members, data_type, comm_op, reduce_op;

    T base_vertex_value;
    sockaddr_in meta_server;

    comm_object() {}
    
    comm_object(
        uint32_t object_id, uint32_t vec_len, bool has_bitmap, uint32_t start_index, 
        bool root, uint8_t members, uint8_t data_type, uint8_t comm_op, uint8_t reduce_op,
        T base_vertex_value, std::string meta_server_addr, int meta_server_port
    ) : object_id(object_id), vec_len(vec_len), start_index(start_index), has_bitmap(has_bitmap), 
        root(root), members(members), data_type(data_type), comm_op(comm_op), reduce_op(reduce_op),
        base_vertex_value(base_vertex_value) {
        flag = caas_build_flag(root, members, data_type, comm_op, reduce_op);
        bitmap_len = has_bitmap ? bitmap::get_bitmap_length_bits(vec_len) >> 5 : 0;
        data = new uint32_t[5 + bitmap_len + vec_len]();
        data[1] = object_id;
        data[2] = bitmap_len;
        data[3] = vec_len;
        data[4] = flag;
        bm = has_bitmap ? new bitmap(vec_len, data + 5) : nullptr;
        vec = (T *)(data + 5 + bitmap_len);
        meta_server.sin_family = AF_INET;
        meta_server.sin_addr.s_addr = inet_addr(meta_server_addr.c_str());
        meta_server.sin_port = htons(meta_server_port);
    }

    virtual void caas_connect(uint32_t request_id) {}

    virtual void caas_disconnect() {}

    virtual void caas_do() {}

    void print(int round, int index) {
        VLOG(2) << "round " << round << " graph " << index << " "
            << "[ " << start_index << ", " << start_index + vec_len - 1 << " ]: " 
            << "bitmap " << bm -> print().str() << " "
            << "value " << log_array<T>(vec, vec_len).str();
    }

};

template <class T>
class proxy : public comm_object<T> {

public:

    sockaddr_in proxy_server;
    int proxy_server_socket;

    proxy() {}

    proxy(
        uint32_t object_id, uint32_t vec_len, bool has_bitmap, uint32_t start_vertex, 
        bool root, uint8_t members, uint8_t data_type, uint8_t comm_op, uint8_t reduce_op,
        T base_vertex_value, std::string meta_server_addr, int meta_server_port
    ): comm_object<T>(
            object_id, vec_len, has_bitmap, start_vertex, 
            root, members, data_type, comm_op, reduce_op,
            base_vertex_value, meta_server_addr, meta_server_port
    ) {}

    void caas_connect(uint32_t request_id) {
        this -> request_id = request_id;
        this -> data[0] = request_id;
        int meta_server_socket = socket(AF_INET, SOCK_STREAM, 0);
        int status_code = connect(meta_server_socket, (sockaddr *)&this -> meta_server, sizeof(sockaddr_in));
        CHECK(status_code == 0) << "can't connect to meta server";
        recv(meta_server_socket, &proxy_server, sizeof(sockaddr_in), 0);
        close(meta_server_socket);
        proxy_server_socket = socket(AF_INET, SOCK_STREAM, 0);
        status_code = connect(proxy_server_socket, (sockaddr *)&proxy_server, sizeof(sockaddr_in));
        CHECK(status_code == 0) << "can't connect to proxy server";
        uint32_t connect_data[6] = {request_id, this -> object_id, this -> vec_len, 
            *(uint32_t *)&this -> base_vertex_value, this -> flag, this -> has_bitmap};
        send(proxy_server_socket, connect_data, sizeof(uint32_t) * 6, 0);
    }

    void caas_disconnect() {
        close(proxy_server_socket);
    }

    void caas_do() {
        timer t;
        if (this -> root) {
            switch (this -> comm_op) {
                case CAAS_MASKED_BROADCAST: {
                    bool segment_type = caas_adaptive_segment<T>(this);
                    std::pair<char *, size_t> segment = caas_make_adaptive_segment<T>(this, segment_type);
                    caas_send_all(proxy_server_socket, segment.first, segment.second);
                    if (segment_type == CAAS_SPARSE) {
                        delete [] segment.first;
                    }
                    break;
                }
                case CAAS_MASKED_REDUCE: {
                    t.tick("recv_all");
                    std::pair<char *, size_t> segment = caas_recv_all(proxy_server_socket);
                    t.from_tick();
                    t.tick("reduce_adaptive_segment");
                    caas_reduce_adaptive_segment<T>(this, segment.first, segment.second);
                    t.from_tick();
                    delete [] segment.first;
                    break;
                }
                default:
                    LOG(FATAL) << "undefined comm op " << this -> comm_op;
                    break;
            }
        } else {
            switch (this -> comm_op) {
                case CAAS_MASKED_BROADCAST: {
                    std::pair<char *, size_t> segment = caas_recv_all(proxy_server_socket);
                    caas_put_adaptive_segment<T>(this, segment.first, segment.second);
                    delete [] segment.first;
                    break;
                }
                case CAAS_MASKED_REDUCE: {
                    bool segment_type = caas_adaptive_segment<T>(this);
                    t.tick("make_adaptive_segment");
                    std::pair<char *, size_t> segment = caas_make_adaptive_segment<T>(this, segment_type);
                    t.from_tick();
                    t.tick("send_all");
                    caas_send_all(proxy_server_socket, segment.first, segment.second);
                    t.from_tick();
                    if (segment_type == CAAS_SPARSE) {
                        delete [] segment.first;
                    }
                    break;
                }
                case CAAS_ALLREDUCE: {
                    std::pair<char *, size_t> send_segment = caas_make_segment(this);
                    caas_send_all(proxy_server_socket, send_segment.first, send_segment.second);
                    std::pair<char *, size_t> recv_segment = caas_recv_all(proxy_server_socket);
                    caas_put_segment<T>(this, recv_segment.first, recv_segment.second);
                    break;
                }
                default:
                    LOG(FATAL) << "undefined comm op " << this -> comm_op;
                    break;
            }
        }
    }

};

template <class T>
comm_object<T> *caas_make_comm_object(
    uint8_t comm_type, std::string meta_server_addr, int meta_server_port,
    uint32_t object_id, uint32_t vec_len, bool has_bitmap, uint32_t start_vertex, 
    bool root, uint8_t members, uint8_t data_type, uint8_t comm_op, uint8_t reduce_op,
    T base_vertex_value
) {
    comm_object<T> *result = nullptr;
    switch (comm_type) {
        case CAAS_PROXY:
            result = new proxy<T>(
                object_id, vec_len, has_bitmap, start_vertex, 
                root, members, data_type, comm_op, reduce_op,
                base_vertex_value, meta_server_addr, meta_server_port
            );
            break;
        default:
            break;
    }
    return result;
}

template <class T>
bool caas_adaptive_segment(comm_object<T> *object) {
    return object -> bm -> get_size() <= CAAS_SPARSE_LIMIT ? CAAS_SPARSE : CAAS_DENSE;
}

template <class T>
std::pair<char *, size_t> caas_make_adaptive_segment(comm_object<T> *object, bool segment_type) {
    caas_segment_set_segment_type(object -> data + 4, segment_type);
    if (segment_type == CAAS_DENSE) {
        return {(char *)object -> data, (5 + object -> bitmap_len + object -> vec_len) << 2};
    } else {
        uint32_t segment_len = 5 + object -> bitmap_len + object -> bm -> get_size();
        uint32_t *segment = new uint32_t[segment_len];
        uint32_t pos = 5 + object -> bitmap_len;
        memcpy(segment, object -> data, 20 + (object -> bitmap_len << 2));
        for (uint32_t i = 0; i < object -> vec_len; i++) {
            if (object -> bm -> exist(i)) {
                segment[pos] = object -> vec[i];
                pos++;
            }
        }
        return {(char *)segment, segment_len << 2};
    }
}

template <class T>
void caas_put_adaptive_segment(comm_object<T> *object, char *data, size_t len) {
    uint32_t *segment = (uint32_t *)data;
    bool segment_type = caas_segment_get_segment_type(segment[4]);
    if (segment_type == CAAS_DENSE) {
        memcpy(object -> data, data, len);
    } else {
        memcpy(object -> data, segment, 20 + (object -> bitmap_len << 2));
        uint32_t pos = 5 + object -> bitmap_len;
        for (uint32_t i = 0; i < object -> vec_len; i++) {
            if (object -> bm -> exist(i)) {
                object -> vec[i] = segment[pos];
                pos++;
            }
        }
    }
}

template <class T>
void caas_reduce_adaptive_segment(comm_object<T> *object, char *data, size_t len) {
    uint32_t *segment = (uint32_t *)data;
    bool segment_type = caas_segment_get_segment_type(segment[4]);
    uint8_t reduce_op = caas_flag_get_reduce_op(segment[4]);
    if (segment_type == CAAS_DENSE) {
        reduce_vec_masked_dense<T>(object -> vec, (T *)segment + 5 + object -> bitmap_len, object -> vec_len, object -> bm, reduce_op);
    } else {
        bitmap *segment_bm = new bitmap(object -> vec_len, segment + 5);
        reduce_vec_masked_sparse<T>(object -> vec, (T *)segment + 5 + object -> bitmap_len, object -> vec_len, object -> bm, segment_bm, reduce_op);
    }
}

template <class T>
std::pair<char *, size_t> caas_make_segment(comm_object<T> *object) {
    return {(char *)object -> data, (5 + object -> vec_len) << 2};
}

template <class T>
void caas_put_segment(comm_object<T> *object, char *data, size_t len) {
    memcpy(object -> data, data, len);
}

#endif