#ifndef _LOG_H
#define _LOG_H

#include <glog/logging.h>

template <class T>
std::ostringstream log_array(T *array, int len) {
    std::ostringstream oss;
    oss << "[ ";
    for (int i = 0; i < len; ++i) {
        oss << array[i];
        if (i < len - 1) {
            oss << ", ";
        }
    }
    oss << " ]";
    return oss;
}

#endif