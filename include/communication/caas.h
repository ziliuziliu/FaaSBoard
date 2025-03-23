#ifndef _CAAS_H
#define _CAAS_H

#include "util/bitmap.h"
#include "util/types.h"
#include "util/reduce.h"
#include "util/log.h"
#include "util/timer.h"
#include "util/flags.h"
#include "util/sdk.h"

#include <vector>
#include <arpa/inet.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

template <class T>
CAAS_TYPE caas_get_data_type() {
    if (std::is_same<int, T>::value) {
        return CAAS_TYPE::INT;
    } else if (std::is_same<float, T>::value) {
        return CAAS_TYPE::FLOAT;
    } else if (std::is_same<uint32_t, T>::value) {
        return CAAS_TYPE::UINT32;
    }
    LOG(FATAL) << "unimplemented type";
}

uint32_t caas_build_flag(bool root, uint8_t instances, uint8_t members, CAAS_TYPE data_type, CAAS_OP comm_op, CAAS_REDUCE_OP reduce_op) {
    uint32_t flag = 0;
    flag |= (root << 31);
    flag |= ((instances & 0xff) << 16);
    flag |= ((members & 0xf) << 12);
    flag |= (((uint8_t)data_type & 0xf) << 8);
    flag |= (((uint8_t)comm_op & 0xf) << 4);
    flag |= ((uint8_t)reduce_op & 0xf);
    return flag;
}

bool caas_flag_get_root(uint32_t flag) {
    return flag >> 31;
}

uint8_t caas_flag_get_instances(uint32_t flag) {
    return (flag >> 16) & 0xff;
}

uint8_t caas_flag_get_members(uint32_t flag) {
    return (flag >> 12) & 0xf;
}

CAAS_TYPE caas_flag_get_data_type(uint32_t flag) {
    return (CAAS_TYPE)((flag >> 8) & 0xf);
}

CAAS_OP caas_flag_get_comm_op(uint32_t flag) {
    return (CAAS_OP)((flag >> 4) & 0xf);
}

CAAS_REDUCE_OP caas_flag_get_reduce_op(uint32_t flag) {
    return (CAAS_REDUCE_OP)(flag & 0xf);
}

COMM_TYPE caas_segment_get_segment_type(uint32_t flag) {
    return (COMM_TYPE)((flag >> 29) & 0x3);
}

void caas_segment_set_segment_type(uint32_t *flag, COMM_TYPE segment_type) {
    *flag &= 0x9fffffff;
    *flag |= ((uint32_t)segment_type << 29);
}

COMM_TYPE caas_adaptive_segment(uint32_t segment_size) {
    if (segment_size == 0) {
        return COMM_TYPE::CAAS_MAGIC;
    } else if (segment_size <= CAAS_SPARSE_PAIR_LIMIT) {
        return COMM_TYPE::CAAS_PAIR;
    } else if (segment_size <= CAAS_SPARSE_LIMIT) {
        return COMM_TYPE::CAAS_SPARSE;
    } else {
        return COMM_TYPE::CAAS_DENSE;
    }
}

void caas_send_all(int fd, char *data, size_t len) {
    size_t pos = 0;
    VLOG(1) << "send:: len = " << len << "; fd = " << fd;
    send(fd, &len, sizeof(size_t), 0);
    while (pos < len) {
        ssize_t send_size = send(fd, data + pos, len - pos, 0);
        pos += (size_t)send_size;
    }
}

std::pair<char *, size_t> caas_recv_all(int fd) {
    size_t len, pos = 0;
    ssize_t signal = recv(fd, &len, sizeof(size_t), 0);
    VLOG(1) << "recv:: len = " << len << "; fd = " << fd;
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
    uint32_t object_id; // tree_id of send or recv
    uint32_t tree_id; // tree_id means the graph's id in the tree it belongs to rather than the id of tree,
    // (object_id, tree_id) represent a node in a certain tree
    // "members" helps to boundary the send/recv peer 
    
    uint32_t bitmap_len;
    uint32_t vec_len;
    // (object_id, tree_id , members) used for binomial
    // | root, 1 bit | segment_type, 2 bit | 5 bit | instances 8 bit | members 4 bit | data_type, 4 bit | comm_op, 4 bit | reduce_op, 4 bit |
    uint32_t flag;

    uint32_t *data;

    uint32_t start_index;
    bitmap *bm;
    T *vec;

    bool has_bitmap, root;
    uint8_t instances, members;// members represents the num of nodes in a certain tree
    CAAS_TYPE data_type;
    CAAS_OP comm_op;
    CAAS_REDUCE_OP reduce_op;

    T base_vertex_value;
    // sockaddr_in meta_server;

    std::vector<int> related_graph_index;
    uint8_t colocated_member, finish;

    exec_config *config;

    comm_object() {}
    
    comm_object(
        uint32_t object_id, uint32_t tree_id, uint32_t vec_len, bool has_bitmap, uint32_t start_index,
        bool root, uint8_t instances, uint8_t members, CAAS_TYPE data_type, CAAS_OP comm_op, CAAS_REDUCE_OP reduce_op,
        T base_vertex_value, exec_config *config
    ) : object_id(object_id), tree_id(tree_id), vec_len(vec_len), start_index(start_index), has_bitmap(has_bitmap),
        root(root), instances(instances), members(members), data_type(data_type), comm_op(comm_op), reduce_op(reduce_op),
        base_vertex_value(base_vertex_value), config(config) {
        flag = caas_build_flag(root, instances, members, data_type, comm_op, reduce_op);
        bitmap_len = has_bitmap ? bitmap::get_bitmap_length_bits(vec_len) >> 5 : 0;
        data = new uint32_t[7 + bitmap_len + vec_len]();
        data[1] = object_id;
        data[2] = bitmap_len;
        data[3] = vec_len;
        data[4] = flag;
        // data[5] -> from_id, data[6] -> to_id
        bm = has_bitmap ? new bitmap(vec_len, data + 7) : nullptr;
        vec = (T *)(data + 7 + bitmap_len);
        // meta_server.sin_family = AF_INET;
        // meta_server.sin_addr.s_addr = inet_addr(config -> meta_server_addr.c_str());
        // meta_server.sin_port = htons(CAAS_META_SERVER_PORT);
        colocated_member = finish = 0;
        related_graph_index = std::vector<int>();
    }

    virtual void caas_connect(uint32_t request_id, uint32_t partition_id) = 0;

    virtual void caas_disconnect() = 0;

    virtual uint32_t caas_do(int round = -1, int total_fds = -1) = 0;

    void update_root() {
        root = true;
        flag = caas_build_flag(root, instances, members, data_type, comm_op, reduce_op);
        data[4] = flag;
    }

    void print(int round) {
        VLOG(2) << "round " << round << " object " << object_id << " "
            << "[ " << start_index << ", " << start_index + vec_len - 1 << " ]: " 
            << "bitmap " << bm -> print().str() << " "
            << "value " << log_array<T>(vec, uint64_t(vec_len)).str();
    }

};

template <class T>
class proxy : public comm_object<T> {

public:

    sockaddr_in proxy_server;
    int proxy_server_socket;
    elasticache_sdk *e_sdk;

    proxy() {}

    proxy(
        uint32_t object_id, uint32_t tree_id, uint32_t vec_len, bool has_bitmap, uint32_t start_vertex, 
        bool root, uint8_t instances, uint8_t members, CAAS_TYPE data_type, CAAS_OP comm_op, CAAS_REDUCE_OP reduce_op,
        T base_vertex_value, exec_config *config
    ): comm_object<T>(
            object_id, tree_id, vec_len, has_bitmap, start_vertex, 
            root, instances, members, data_type, comm_op, reduce_op,
            base_vertex_value, config
    ) {
        e_sdk = new elasticache_sdk(config -> elasticache_host, 6379);
    }

    ~proxy() {
        e_sdk -> close(); // Close ElastiCache connection
    }

    void caas_connect(uint32_t request_id, uint32_t partition_id) {
        this -> request_id = request_id;
        this -> data[0] = request_id;

        // int meta_server_socket = socket(AF_INET, SOCK_STREAM, 0);
        // int status_code = connect(meta_server_socket, (sockaddr *)&this -> meta_server, sizeof(sockaddr_in));
        // CHECK(status_code == 0) << "can't connect to meta server";
        // recv(meta_server_socket, &proxy_server, sizeof(sockaddr_in), 0);
        // close(meta_server_socket);

        std::string proxy_server_host;
        if (this -> config -> elastic_proxy) {
            VLOG(1) << "before get proxy ip from elasticache";
            e_sdk -> connect();
            proxy_server_host = e_sdk -> get(std::to_string(this -> request_id));
            if (proxy_server_host == "") {
                std::cout << "request " << this -> request_id << ": get proxy server address list from GlobalIPList" << std::endl;
                std::vector<std::string> proxy_server_ips = e_sdk -> getList("GlobalIPList");
                std::cout << "request " << this -> request_id << ": proxy server list size " << proxy_server_ips.size() << std::endl;
                srand(time(0));
                uint32_t idx = rand() % proxy_server_ips.size();
                bool success = e_sdk -> setnx(std::to_string(this -> request_id), (proxy_server_ips[idx].c_str()));
                proxy_server_host = e_sdk -> get(std::to_string(this -> request_id));
                if (!success) {
                    assert(proxy_server_host != "");
                } else {
                    assert(proxy_server_host == proxy_server_ips[idx]);
                }
                VLOG(1) << "request " << this -> request_id << ": proxy server " << proxy_server_host;
            }
            VLOG(1) << "after get proxy ip from elasticache";
        } else {
            proxy_server_host = this -> config -> proxy_ip;
        }

        proxy_server.sin_family = AF_INET;
        proxy_server.sin_addr.s_addr = inet_addr(proxy_server_host.c_str());
        proxy_server.sin_port = htons(20001);

        proxy_server_socket = socket(AF_INET, SOCK_STREAM, 0);
        int status_code = connect(proxy_server_socket, (sockaddr *)&proxy_server, sizeof(sockaddr_in));
        VLOG(1) << "Proxy server address: " << inet_ntoa(proxy_server.sin_addr);
        CHECK(status_code == 0) << "can't connect to proxy server";
        uint32_t connect_data[8] = {request_id, partition_id, this -> object_id, this -> tree_id,this -> vec_len, 
            *(uint32_t *)&this -> base_vertex_value, this -> flag, this -> has_bitmap};
        send(proxy_server_socket, connect_data, sizeof(uint32_t) * 8, 0);
    }

    void caas_disconnect() {
        VLOG(1) << "disconnect object " << this -> object_id;
        // uint32_t kill_msg = CAAS_KILL_MESSAGE;
        // caas_send_all(proxy_server_socket, (char *)&kill_msg, sizeof(uint32_t));
        close(proxy_server_socket);
    }

    uint32_t caas_do(int round = -1, int total_fds = -1) {
        VLOG(1) << "come in caas_do";
        VLOG(1) << "this -> reduce_op = " << (int)this -> reduce_op;
        if (this -> comm_op == CAAS_OP::MASKED_BROADCAST || this -> comm_op == CAAS_OP::MASKED_REDUCE) {
            if (this -> colocated_member == this -> members) {
                VLOG(1) << "object " << this -> object_id << " return from " << (int)(this -> comm_op);
                return 0;
            }
        }

        switch (this -> comm_op){
            case CAAS_OP::MASKED_BROADCAST: { // root or not, it's ok
                VLOG(1) << "caas::MASKED_BROADCAST";
                int rounds = ceil(log2(int(this -> instances)));
                VLOG(1) << "this -> instances = " << (int)this -> instances;
                for(int i = rounds - 1 ; i >= 0; i--){
                    VLOG(1) << "i = " << i;
                    uint32_t rcpt_id = this -> tree_id + uint32_t(std::pow(2,i));// for root, tree_id = 0 
                    VLOG(1) << "this -> tree_id = " << this -> tree_id << "; rcpt_id = " << rcpt_id;
                    if (this -> tree_id % uint32_t(std::pow(2,i+1)) == 0 && rcpt_id < this -> instances) {
                        VLOG(1) << "broadcast:: send; rcpt_id = " << rcpt_id;
                        COMM_TYPE segment_type = caas_adaptive_segment(this -> bm -> get_size());
                        std::pair<char *, size_t> send_segment = caas_make_adaptive_segment<T>(this, segment_type);
                        uint32_t *send_data = (uint32_t *)send_segment.first;
                        send_data[5] = this -> tree_id; // from_id
                        send_data[6] = rcpt_id; // to_id
                        
                        VLOG(1) << "broadcast:: before send";
                        caas_send_all(proxy_server_socket, send_segment.first, send_segment.second);
                        VLOG(1) << "broadcast:: send over";
                        if (segment_type != COMM_TYPE::CAAS_DENSE) {
                            delete [] send_segment.first;
                        }
                    } else if (this -> tree_id % uint32_t(std::pow(2,i)) == 0 && this -> tree_id % uint32_t(std::pow(2,i+1)) != 0){
                        uint32_t src_id = this -> tree_id - uint32_t(std::pow(2,i)); 
                        VLOG(1) << "broadcast:: recv; src_id = " << src_id;
                        // do not need src_id actually, but it helps to understand the mechanism
                        VLOG(1) << "broadcast:: before recv";
                        std::pair<char *, size_t> recv_segment = caas_recv_all(proxy_server_socket);
                        VLOG(1) << "broadcast:: recv over";
                        uint32_t *recv_data = (uint32_t *) recv_segment.first;
                        recv_data[5] = src_id; //from_id
                        recv_data[6] = this -> tree_id; // recv_id on the same tree
                        caas_put_adaptive_segment<T>(this, recv_segment.first, recv_segment.second);
                        delete [] recv_segment.first;
                    }
                }
                VLOG(1) << "caas::out MASKED_BROADCAST";
                break;
            }

            case CAAS_OP::MASKED_REDUCE: {
                VLOG(1) << "caas::MASKED_REDUCE";
                int rounds = ceil(log2(int(this -> instances)));
                for(int i = 0; i < rounds; i++) {
                    VLOG(1) << "i = " << i;
                    uint32_t src_id = this -> tree_id + uint32_t(std::pow(2,i));// for root, tree_id == 0
                    VLOG(1) << "this -> tree_id = " << this -> tree_id << "; src_id = " << src_id;
                    if (this -> tree_id % uint32_t(std::pow(2,i+1)) == 0 && src_id < this -> instances) {
                        VLOG(1) << "before recv";
                        std::pair<char *, size_t> recv_segment = caas_recv_all(proxy_server_socket);
                        VLOG(1) << "after recv";
                        uint32_t *recv_data = (uint32_t *) recv_segment.first;
                        recv_data[5] = src_id; //from_id
                        recv_data[6] = this -> tree_id; // recv_id on the same tree
                        CAAS_REDUCE_OP reduce_op = caas_flag_get_reduce_op(recv_data[4]);
                        VLOG(1) << "masked_reduce:: reduce_op  = " << (int)reduce_op << "; the_reduce_op = " << (int)this -> reduce_op << "; this -> tree_id = " << this -> tree_id << "; src_id = " << src_id;
                        VLOG(1) << "the_reduce_op = " << (int)this -> reduce_op;
                        caas_reduce_adaptive_segment<T>(this, recv_segment.first, recv_segment.second);
                        delete [] recv_segment.first;
                    } else if (this -> tree_id % uint32_t(std::pow(2,i)) == 0 && this -> tree_id % uint32_t(std::pow(2,i+1)) != 0){
                        uint32_t rcpt_id = this -> tree_id - (uint32_t)std::pow(2,i);
                        VLOG(1) << "this -> tree_id = " << this -> tree_id << "; rcpt_id = " << rcpt_id;
                        COMM_TYPE segment_type = caas_adaptive_segment(this -> bm -> get_size());
                        std::pair<char *, size_t> send_segment = caas_make_adaptive_segment<T>(this, segment_type);
                        uint32_t *send_data = (uint32_t *)send_segment.first;
                        send_data[5] = this -> tree_id; // from_id
                        send_data[6] = rcpt_id; // to_id
                        VLOG(1) << "before send";
                        caas_send_all(proxy_server_socket, send_segment.first, send_segment.second);
                        VLOG(1) << "after send";
                        delete [] send_segment.first;
                        // if (segment_type != COMM_TYPE::CAAS_DENSE) {
                        //     delete [] send_segment.first;
                        // }
                    }
                }
                VLOG(1) << "caas::out MASKED_REDUCE";
                break;
            }


            case CAAS_OP::ALLREDUCE: {
                std::tuple<char *, size_t, bool> send_segment = caas_make_segment(this, round, total_fds);
                caas_send_all(proxy_server_socket, std::get<0>(send_segment), std::get<1>(send_segment));
                if (std::get<2>(send_segment)) {
                    delete [] std::get<0>(send_segment);
                }
                std::pair<char *, size_t> recv_segment = caas_recv_all(proxy_server_socket);
                if (*(uint32_t *)recv_segment.first == CAAS_KILL_MESSAGE) {
                    return CAAS_KILL_MESSAGE;
                }
                caas_put_segment<T>(this, recv_segment.first, recv_segment.second);
                delete [] recv_segment.first;
                break;
            }

        }
        return 0;
    }

};

template <class T>
comm_object<T> *caas_make_comm_object(
    CAAS_COMM_MODE comm_type, uint32_t object_id, uint32_t tree_id, uint32_t vec_len, bool has_bitmap, uint32_t start_vertex, 
    bool root, uint8_t instances, uint8_t members, CAAS_TYPE data_type, CAAS_OP comm_op, CAAS_REDUCE_OP reduce_op,
    T base_vertex_value, exec_config *config
) {
    comm_object<T> *result = nullptr;
    switch (comm_type) {
        case CAAS_COMM_MODE::PROXY:
            result = new proxy<T>(
                object_id, tree_id, vec_len, has_bitmap, start_vertex, 
                root, instances, members, data_type, comm_op, reduce_op,
                base_vertex_value, config
            );
            break;
        default:
            break;
    }
    return result;
}

template <class T>
std::pair<char *, size_t> caas_make_adaptive_segment(comm_object<T> *object, COMM_TYPE segment_type) {
    // Set segment type
    caas_segment_set_segment_type(object -> data + 4, segment_type);

    switch(segment_type) {
        case COMM_TYPE::CAAS_MAGIC: {
            uint32_t seg_pairs_len = 7;
            uint32_t *seg_pairs = new uint32_t[seg_pairs_len];
            memcpy(seg_pairs, object -> data, 28); // 28 is the size of the first 7 elements
            return {(char *)seg_pairs, 7 << 2};
        }

        case COMM_TYPE::CAAS_PAIR: {
            // Construct sparse segment with only index-value pairs whose value is not zero
            uint32_t seg_pairs_len = 7 + object -> bm -> get_size() * 2;
            uint32_t *seg_pairs = new uint32_t[seg_pairs_len];
            uint32_t pos = 7;
            memcpy(seg_pairs, object -> data, 28); // 28 is the size of the first 7 elements
            bitmap_iterator *it = new bitmap_iterator(object -> bm, object -> vec_len);
            for (;;) {
                uint32_t index = it -> next();
                if (index == 0xffffffff) {
                    break;
                }
                seg_pairs[pos] = index;
                seg_pairs[pos + 1] = object -> data[7 + object -> bitmap_len + index];
                pos += 2;
            }
            return {(char *)seg_pairs, pos << 2};
        }

        case COMM_TYPE::CAAS_SPARSE: {
            uint32_t segment_len = 7 + object -> bitmap_len + object -> bm -> get_size();
            uint32_t *segment = new uint32_t[segment_len];
            uint32_t pos = 7 + object -> bitmap_len;
            memcpy(segment, object -> data, 28 + (object -> bitmap_len << 2));
            bitmap_iterator *it = new bitmap_iterator(object -> bm, object -> vec_len);
            for (;;) {
                uint32_t index = it -> next();
                if (index == 0xffffffff) {
                    break;
                }
                segment[pos] = object -> data[7 + object -> bitmap_len + index];
                pos++;
            }
            return {(char *)segment, segment_len << 2};
        }
       
        case COMM_TYPE::CAAS_DENSE: {
            return {(char *)object -> data, (7 + object -> bitmap_len + object -> vec_len) << 2};
        }

        default: {
            LOG(FATAL) << "undefined segment type " << (int)segment_type;
        }
    }
}

template <class T>
void caas_put_adaptive_segment(comm_object<T> *object, char *data, size_t len) {
    uint32_t *segment = (uint32_t *)data; 
    COMM_TYPE segment_type = caas_segment_get_segment_type(segment[4]);
    
    switch (segment_type) {
        case COMM_TYPE::CAAS_MAGIC: {
            object -> bm -> clear();
            break;
        }

        case COMM_TYPE::CAAS_PAIR: {
            memcpy(object -> data, segment, 28);
            uint32_t pos = 7;
            // Clear current bitmap
            object -> bm -> clear();
            while (pos < (len >> 2)) {
                // Update bitmap and vector
                uint32_t index = segment[pos];
                object -> data[7 + object -> bitmap_len + index] = segment[pos + 1];
                object -> bm -> add(index);
                pos += 2;
            }
            break;
        }

        case COMM_TYPE::CAAS_SPARSE: {
            memcpy(object -> data, segment, 28 + (object -> bitmap_len << 2));
            uint32_t pos = 7 + object -> bitmap_len;
            bitmap_iterator *it = new bitmap_iterator(object -> bm, object -> vec_len);
            for (;;) {
                uint32_t index = it -> next();
                if (index == 0xffffffff) {
                    break;
                }
                object -> data[7 + object -> bitmap_len + index] = segment[pos];
                pos++;
            }
            break;
        }

        case COMM_TYPE::CAAS_DENSE: {
            memcpy(object -> data, data, len);
            break;
        }

        default: {
            LOG(FATAL) << "undefined segment type " << (int)segment_type;
        }
    }
}

template <class T>
void caas_reduce_adaptive_segment(comm_object<T> *object, char *data, size_t len) {
    uint32_t *segment = (uint32_t *)data;
    COMM_TYPE segment_type = caas_segment_get_segment_type(segment[4]);
    CAAS_REDUCE_OP reduce_op = caas_flag_get_reduce_op(segment[4]);

    switch (segment_type) {
        case COMM_TYPE::CAAS_MAGIC: {
            return;
        }

        case COMM_TYPE::CAAS_PAIR: {
            reduce_vec_masked_sparse_pair<T>(object -> vec, (T *)segment + 7, object -> vec_len, (len >> 2) - 7, object -> bm, reduce_op);
            break;
        }

        case COMM_TYPE::CAAS_SPARSE: {
            bitmap *segment_bm = new bitmap(object -> vec_len, segment + 7);
            reduce_vec_masked_sparse<T>(object -> vec, (T *)segment + 7 + object -> bitmap_len, object -> vec_len, object -> bm, segment_bm, reduce_op);
            break;
        }

        case COMM_TYPE::CAAS_DENSE: {
            VLOG(1) << "caas_reduce_adaptive_segment:: reduce_op = " << (int)reduce_op;
            reduce_vec_masked_dense<T>(object -> vec, (T *)segment + 7 + object -> bitmap_len, object -> vec_len, object -> bm, reduce_op);
            break;
        }
    
        default: {
            LOG(FATAL) << "undefined segment type " << (int)segment_type;
        }
    }
}

template <class T>
std::tuple<char *, size_t, bool> caas_make_segment(comm_object<T> *object, int round, int total_fds) {
    int data_len = (7 + object -> vec_len) << 2;
    return std::make_tuple((char *)object -> data, data_len, false);
}

template <class T>
void caas_put_segment(comm_object<T> *object, char *data, size_t len) {
    memcpy(object -> data, data, len);
}

#endif