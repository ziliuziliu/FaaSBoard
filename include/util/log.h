#ifndef _LOG_H
#define _LOG_H

#include "types.h"

#include <glog/logging.h>

template <class T>
std::ostringstream log_array(T *array, uint64_t len) {
    std::ostringstream oss;
    oss << "[ ";
    for (uint64_t i = 0; i < len; ++i) {
        oss << array[i];
        if (i < len - 1) {
            oss << ", ";
        }
    }
    oss << " ]";
    return oss;
}

std::string log_comm_type(COMM_TYPE type) {
    switch (type) {
        case COMM_TYPE::CAAS_MAGIC:
            return "CAAS_MAGIC";
        case COMM_TYPE::CAAS_PAIR:
            return "CAAS_PAIR";
        case COMM_TYPE::CAAS_SPARSE:
            return "CAAS_SPARSE";
        case COMM_TYPE::CAAS_DENSE:
            return "CAAS_DENSE";
        default:
            return "UNKNOWN";
    }
}

#endif