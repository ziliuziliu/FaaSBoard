#ifndef _BITMAP_H
#define _BITMAP_H

#include <util/log.h>

#include <cstdint>
#include <cstring>

struct bitmap {
    
    uint32_t length;
    uint32_t *m, *size;

    bitmap() {}

    bitmap(uint32_t length) {
        this -> length = get_bitmap_length_bits(length);
        this -> m = new uint32_t[this -> length >> 5]();
        this -> size = this -> m;
    }

    bitmap(uint32_t length, uint32_t *m) {
        this -> length = get_bitmap_length_bits(length);
        this -> m = this -> size = m;
    }

    ~bitmap() {
        delete [] m;
    }

    static uint32_t get_bitmap_length_bits(uint32_t length) {
        return ((length >> 5) + 2) << 5;
    }

    uint32_t get_size() {
        return *size;
    }

    bool exist(uint32_t x) {
        uint32_t slot = x >> 5, bit = x & 0x1f;
        return m[slot + 1] & (1 << bit);
    }

    void add(uint32_t x) {
        uint32_t slot = x >> 5, bit = x & 0x1f;
        __sync_fetch_and_or(&m[slot + 1], 1 << bit);
        __sync_fetch_and_add(size, 1);
    }

    void clear() {
        memset(m, 0, length >> 3);
    }

    bitmap *OR(bitmap *b) {
        bitmap *result = new bitmap(length);
        for (int i = 1; i < (int)length >> 5; i++) {
            result -> m[i] = m[i] | b -> m[i];
            *(result -> size) += __builtin_popcount(result -> m[i]);
        }
        return result;
    }

    bitmap *AND(bitmap *b) {
        bitmap *result = new bitmap(length);
        for (int i = 1; i < (int)length >> 5; i++) {
            result -> m[i] = m[i] & b -> m[i];
            *(result -> size) += __builtin_popcount(result -> m[i]);
        }
        return result;
    }

    bitmap *NOT() {
        bitmap *result = new bitmap(length);
        for (int i = 1; i < (int)length >> 5; i++) {
            result -> m[i] = ~m[i];
            *(result -> size) += __builtin_popcount(result -> m[i]);
        }
        return result;
    }

    std::ostringstream print() {
        std::ostringstream oss;
        oss << "[ ";
        for (uint32_t i = 0; i < length - 32; i++) {
            oss << i << ": ";
            if (exist(i)) {
                oss << "1";
            } else {
                oss << "0";
            }
            if (i < length - 1) {
                oss << ", ";
            }
        }
        oss << " ]";
        return oss;
    }

};

#endif