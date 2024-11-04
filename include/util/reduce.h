#ifndef _REDUCE_H
#define _REDUCE_H

#include "util/types.h"
#include "util/log.h"
#include "util/bitmap.h"

#include <cstdint>
#include <type_traits>
#include <functional>
#include <iostream>
#include <immintrin.h>
#include <bitset>

using reduce_uint32_f_single = std::function<uint32_t(uint32_t, uint32_t)>;
using reduce_int_f_single = std::function<int(int, int)>;
using reduce_float_f_single = std::function<float(float, float)>;
using reduce_uint32_f_avx_masked = std::function<void(uint32_t *, uint32_t *, uint8_t *, uint32_t)>;

reduce_uint32_f_single get_reduce_func_uint32_single(uint8_t reduce_op) {
    switch (reduce_op) {
        case CAAS_UP:
            return reduce_uint32_f_single([](uint32_t x, uint32_t y) -> uint32_t {
                if (x != 0xffffffff) {
                    return x;
                } else if (y != 0xffffffff) {
                    return y;
                } else {
                    return 0xffffffff;
                }
            });
        case CAAS_ADD:
            return reduce_uint32_f_single([](uint32_t x, uint32_t y) -> uint32_t {
                return x + y;
            });
        case CAAS_MIN:
            return reduce_uint32_f_single([](uint32_t x, uint32_t y) -> uint32_t {
                return std::min(x, y);
            });
        default:
            LOG(FATAL) << "reduce op " << reduce_op << " not implemented";
    }
}

reduce_int_f_single get_reduce_func_int_single(uint8_t reduce_op) {
    switch (reduce_op) {
        case CAAS_ADD:
            return reduce_int_f_single([](int x, int y) -> int {
                return x + y;
            });
        case CAAS_MIN:
            return reduce_int_f_single([](int x, int y) -> int {
                return std::min(x, y);
            });
        default:
            LOG(FATAL) << "reduce op " << reduce_op << " not implemented";
    }
}

reduce_float_f_single get_reduce_func_float_single(uint8_t reduce_op) {
    switch (reduce_op) {
        case CAAS_ADD:
            return reduce_float_f_single([](float x, float y) -> float {
                return x + y;
            });
        case CAAS_MIN:
            return reduce_float_f_single([](float x, float y) -> float {
                return std::min(x, y);
            });
        default:
            LOG(FATAL) << "reduce op " << reduce_op << " not implemented";
    }
}

void print_m256i_bits(__m256i vec) {
    // Store the contents of the __m256i register into a 32-byte array
    alignas(32) uint32_t elements[8];
    _mm256_storeu_si256((__m256i*)elements, vec);

    // Print each element's bits
    for (int i = 0; i < 8; ++i) {
        std::cout << "Element " << i << ": " << std::bitset<32>(elements[i]) << "\n";
    }
}

reduce_uint32_f_avx_masked get_reduce_func_uint32_avx_masked(uint8_t reduce_op) {
    switch (reduce_op) {
        case CAAS_UP:
            return reduce_uint32_f_avx_masked([](uint32_t *a, uint32_t *b, uint8_t *bm, uint32_t len){
                __m256i ones = _mm256_set1_epi32(0xffffffff);
                for (uint32_t i = 0; i < len; i += 8) {
                    __m256i aa = _mm256_loadu_si256((__m256i *)&a[i]);
                    __m256i bb = _mm256_loadu_si256((__m256i *)&b[i]);
                    __m256i amask = _mm256_cmpeq_epi32(aa, ones);
                    __m256i bmask = _mm256_cmpeq_epi32(bb, ones);
                    bmask = _mm256_xor_si256(bmask, ones);
                    __m256i condition = _mm256_and_si256(amask, bmask);
                    __m256i cc = _mm256_blendv_epi8(aa, bb, condition);
                    _mm256_storeu_si256((__m256i *)&a[i], cc);
                    int change = _mm256_movemask_ps(_mm256_castsi256_ps(condition));
                    *bm |= (uint8_t)change;
                    bm++;
                }
            });
        case CAAS_MIN:
            return reduce_uint32_f_avx_masked([](uint32_t *a, uint32_t *b, uint8_t *bm, uint32_t len){
                __m256i ones = _mm256_set1_epi32(0xffffffff);
                for (uint32_t i = 0; i < len; i += 8) {
                    __m256i aa = _mm256_loadu_si256((__m256i *)&a[i]);
                    __m256i bb = _mm256_loadu_si256((__m256i *)&b[i]);
                    __m256i cc = _mm256_min_epu32(aa, bb);
                    __m256i mask = _mm256_cmpeq_epi32(aa, cc);
                    mask = _mm256_xor_si256(mask, ones);
                    _mm256_storeu_si256((__m256i *)&a[i], cc);
                    int change = _mm256_movemask_ps(_mm256_castsi256_ps(mask));
                    *bm |= (uint8_t)change;
                    bm++;
                }
            });
        default:
            LOG(FATAL) << "reduce op " << reduce_op << " not implemented";
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
            reduce_uint32_f_single f = get_reduce_func_uint32_single(reduce_op);
            uint32_t *aa = (uint32_t *)a, *bb = (uint32_t *)b;
            #pragma omp parallel for
            for (uint32_t i = 0; i < len; i++) {
                aa[i] = f(aa[i], bb[i]);
            }
            break;
        }
        case CAAS_INT: {
            reduce_int_f_single f = get_reduce_func_int_single(reduce_op);
            int *aa = (int *)a, *bb = (int *)b;
            #pragma omp parallel for
            for (uint32_t i = 0; i < len; i++) {
                aa[i] = f(aa[i], bb[i]);
            }
            break;
        }
        case CAAS_FLOAT: {
            reduce_float_f_single f = get_reduce_func_float_single(reduce_op);
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
            reduce_uint32_f_avx_masked f = get_reduce_func_uint32_avx_masked(reduce_op);
            f((uint32_t *)a, (uint32_t *)b, (uint8_t *)(a_bm -> m + 1), len);
            a_bm -> refresh_size();
            break;
        }
        case CAAS_INT: {
            reduce_int_f_single f = get_reduce_func_int_single(reduce_op);
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
            reduce_float_f_single f = get_reduce_func_float_single(reduce_op);
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
            reduce_uint32_f_single f = get_reduce_func_uint32_single(reduce_op);
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
            reduce_int_f_single f = get_reduce_func_int_single(reduce_op);
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
            reduce_float_f_single f = get_reduce_func_float_single(reduce_op);
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

template <class T>
void reduce_vec_masked_sparse_pair(T *a, T *b, uint32_t a_len, uint32_t b_len, bitmap *a_bm, uint8_t reduce_op, uint8_t data_type = 0xff) {
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
            reduce_uint32_f_single f = get_reduce_func_uint32_single(reduce_op);
            uint32_t *aa = (uint32_t *)a, *bb = (uint32_t *)b;
            uint32_t idx = 0;
            // b is a pair of index and value, so we need to use the index to update a
            while (idx < b_len) {
                uint32_t index = bb[idx];
                uint32_t new_val = f(aa[index], bb[idx + 1]);
                if (new_val != aa[index]) {
                    a_bm -> add(index);
                }
                aa[index] = new_val;
                idx += 2;
            }
            break;
        }
        case CAAS_INT: {
            reduce_int_f_single f = get_reduce_func_int_single(reduce_op);
            int *aa = (int *)a, *bb = (int *)b;
            uint32_t idx = 0;
            while (idx < b_len) {
                uint32_t index = bb[idx];
                int new_val = f(aa[index], bb[idx + 1]);
                if (new_val != aa[index]) {
                    a_bm -> add(index);
                }
                aa[index] = new_val;
                idx += 2;
            }
            break;
        }
        case CAAS_FLOAT: {
            reduce_float_f_single f = get_reduce_func_float_single(reduce_op);
            float *aa = (float *)a, *bb = (float *)b;
            uint32_t idx = 0;
            while (idx < b_len) {
                uint32_t index = bb[idx];
                float new_val = f(aa[index], bb[idx + 1]);
                if (new_val != aa[index]) {
                    a_bm -> add(index);
                }
                aa[index] = new_val;
                idx += 2;
            }
            break;
        }
        default:
            break;
    }
}

#endif