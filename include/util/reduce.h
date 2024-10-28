#ifndef _REDUCE_H
#define _REDUCE_H

#include "util/types.h"
#include "util/log.h"

#include <cstdint>
#include <type_traits>
#include <functional>
#include <iostream>

using reduce_uint32_f = std::function<uint32_t(uint32_t, uint32_t)>;
using reduce_int_f = std::function<int(int, int)>;
using reduce_float_f = std::function<float(float, float)>;

reduce_uint32_f get_reduce_func_uint32(uint8_t reduce_op) {
    switch (reduce_op) {
        case CAAS_UP:
            return reduce_uint32_f([](uint32_t x, uint32_t y) -> uint32_t {
                if (x != 0xffffffff) {
                    return x;
                } else if (y != 0xffffffff) {
                    return y;
                } else {
                    return 0xffffffff;
                }
            });
        case CAAS_ADD:
            return reduce_uint32_f([](uint32_t x, uint32_t y) -> uint32_t {
                return x + y;
            });
        case CAAS_MIN:
            return reduce_uint32_f([](uint32_t x, uint32_t y) -> uint32_t {
                return std::min(x, y);
            });
        default:
            return reduce_uint32_f([](uint32_t x, uint32_t y) -> uint32_t {
                return 0;
            });
    }
}

reduce_int_f get_reduce_func_int(uint8_t reduce_op) {
    switch (reduce_op) {
        case CAAS_ADD:
            return reduce_int_f([](int x, int y) -> int {
                return x + y;
            });
        case CAAS_MIN:
            return reduce_int_f([](int x, int y) -> int {
                return std::min(x, y);
            });
        default:
            return reduce_int_f([](int x, int y) -> int {
                return 0;
            });
    }
}

reduce_float_f get_reduce_func_float(uint8_t reduce_op) {
    switch (reduce_op) {
        case CAAS_ADD:
            return reduce_float_f([](float x, float y) -> float {
                return x + y;
            });
        case CAAS_MIN:
            return reduce_float_f([](float x, float y) -> float {
                return std::min(x, y);
            });
        default:
            return reduce_float_f([](float x, float y) -> float {
                return 0;
            });
    }
}

template <class T = empty>
void reduce_vec(T *a, T *b, uint32_t len, uint8_t reduce_op, uint8_t data_type = 0xff) {
    if (data_type == 0xff) {
        if (std::is_same<T, int>::value) {
            data_type = CAAS_INT;
        } else if (std::is_same<T, float>::value) {
            data_type = CAAS_FLOAT;
        } else {
            data_type = CAAS_UINT32;
        }
    }
    switch (data_type) {
        case CAAS_UINT32: {
            reduce_uint32_f f = get_reduce_func_uint32(reduce_op);
            uint32_t *aa = (uint32_t *)a, *bb = (uint32_t *)b;
            #pragma omp parallel for
            for (uint32_t i = 0; i < len; i++) {
                aa[i] = f(aa[i], bb[i]);
            }
            break;
        }
        case CAAS_INT: {
            reduce_int_f f = get_reduce_func_int(reduce_op);
            int *aa = (int *)a, *bb = (int *)b;
            #pragma omp parallel for
            for (uint32_t i = 0; i < len; i++) {
                aa[i] = f(aa[i], bb[i]);
            }
            break;
        }
        case CAAS_FLOAT: {
            reduce_float_f f = get_reduce_func_float(reduce_op);
            float *aa = (float *)a, *bb = (float *)b;
            #pragma omp parallel for
            for (uint32_t i = 0; i < len; i++) {
                aa[i] = f(aa[i], bb[i]);
            }
            break;
        }
        default:
            break;
    }
}

template <class T = empty>
void reduce_vec_masked_dense(T *a, T *b, uint32_t len, bitmap *a_bm, uint8_t reduce_op, uint8_t data_type = 0xff) {
    LOG(INFO) << "reduce_dense";
    if (data_type == 0xff) {
        if (std::is_same<T, int>::value) {
            data_type = CAAS_INT;
        } else if (std::is_same<T, float>::value) {
            data_type = CAAS_FLOAT;
        } else {
            data_type = CAAS_UINT32;
        }
    }
    switch (data_type) {
        case CAAS_UINT32: {
            reduce_uint32_f f = get_reduce_func_uint32(reduce_op);
            uint32_t *aa = (uint32_t *)a, *bb = (uint32_t *)b;
            #pragma omp parallel for
            for (uint32_t i = 0; i < len; i++) {
                uint32_t new_val = f(aa[i], bb[i]);
                if (new_val != aa[i]) {
                    a_bm -> add(i);
                }
                aa[i] = new_val;
            }
            break;
        }
        case CAAS_INT: {
            reduce_int_f f = get_reduce_func_int(reduce_op);
            int *aa = (int *)a, *bb = (int *)b;
            #pragma omp parallel for
            for (uint32_t i = 0; i < len; i++) {
                int new_val = f(aa[i], bb[i]);
                if (new_val != aa[i]) {
                    a_bm -> add(i);
                }
                aa[i] = new_val;
            }
            break;
        }
        case CAAS_FLOAT: {
            reduce_float_f f = get_reduce_func_float(reduce_op);
            float *aa = (float *)a, *bb = (float *)b;
            #pragma omp parallel for
            for (uint32_t i = 0; i < len; i++) {
                float new_val = f(aa[i], bb[i]);
                if (new_val != aa[i]) {
                    a_bm -> add(i);
                }
                aa[i] = new_val;
            }
            break;
        }
        default:
            break;
    }
}

template <class T>
void reduce_vec_masked_sparse(T *a, T *b, uint32_t len, bitmap *a_bm, bitmap *b_bm, uint8_t reduce_op, uint8_t data_type = 0xff) {
    LOG(INFO) << "reduce_sparse, b_bm size " << b_bm -> get_size();
    if (data_type == 0xff) {
        if (std::is_same<T, int>::value) {
            data_type = CAAS_INT;
        } else if (std::is_same<T, float>::value) {
            data_type = CAAS_FLOAT;
        } else {
            data_type = CAAS_UINT32;
        }
    }
    bitmap_iterator *it = new bitmap_iterator(b_bm, len);
    switch (data_type) {
        case CAAS_UINT32: {
            reduce_uint32_f f = get_reduce_func_uint32(reduce_op);
            uint32_t *aa = (uint32_t *)a, *bb = (uint32_t *)b;
            uint32_t pos = 0;
            for (;;) {
                uint32_t index = it -> next();
                if (index == 0xffffffff) {
                    break;
                }
                uint32_t new_val = f(aa[index], bb[pos]);
                if (new_val != aa[index]) {
                    a_bm -> add(index);
                }
                aa[index] = new_val;
                pos++;
            }
            break;
        }
        case CAAS_INT: {
            reduce_int_f f = get_reduce_func_int(reduce_op);
            int *aa = (int *)a, *bb = (int *)b;
            uint32_t pos = 0;
            for (;;) {
                uint32_t index = it -> next();
                if (index == 0xffffffff) {
                    break;
                }
                int new_val = f(aa[index], bb[pos]);
                if (new_val != aa[index]) {
                    a_bm -> add(index);
                }
                aa[index] = new_val;
                pos++;
            }
            break;
        }
        case CAAS_FLOAT: {
            reduce_float_f f = get_reduce_func_float(reduce_op);
            float *aa = (float *)a, *bb = (float *)b;
            uint32_t pos = 0;
            for (;;) {
                uint32_t index = it -> next();
                if (index == 0xffffffff) {
                    break;
                }
                float new_val = f(aa[index], bb[pos]);
                if (new_val != aa[index]) {
                    a_bm -> add(index);
                }
                aa[index] = new_val;
                pos++;
            }
            break;
        }
        default:
            break;
    }
}

#endif