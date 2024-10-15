#ifndef ATOMIC_H
#define ATOMIC_H

#include "log.h"

template <class T>
inline bool cas(T *ptr, T old_val, T new_val) {
    if (sizeof(T) == 8) {
        return __sync_bool_compare_and_swap((long*)ptr, *((long*)&old_val), *((long*)&new_val));
    } else if (sizeof(T) == 4) {
        return __sync_bool_compare_and_swap((int*)ptr, *((int*)&old_val), *((int*)&new_val));
    } else {
        LOG(FATAL) << "no valid cas data type";
    }
}

#endif