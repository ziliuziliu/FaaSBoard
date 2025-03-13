#ifndef _LOG_H
#define _LOG_H

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

#endif