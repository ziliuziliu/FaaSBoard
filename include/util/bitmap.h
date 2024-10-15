#ifndef _BITMAP_H
#define _BITMAP_H

#include <util/log.h>

#include <cstdint>
#include <cstring>

struct bitmap {
    
    uint32_t size, length;
    uint32_t *m;

    bitmap() {}

    bitmap(uint32_t length) {
        this -> size = 0;
        this -> length = ((length >> 5) + 1) << 5;
        this -> m = new uint32_t[this -> length >> 5]();
    }

    bitmap(uint32_t length, uint32_t *m) {
        this -> length = ((length >> 5) + 1) << 5;
        this -> m = m;
    }

    ~bitmap() {
        delete [] m;
    }

    bool exist(uint32_t x) {
        uint32_t slot = x >> 5, bit = x & 0x1f;
        return m[slot] & (1 << bit);
    }

    void add(uint32_t x) {
        __sync_fetch_and_or(&m[x >> 5], 1 << (x & 0x1f));
        __sync_fetch_and_add(&size, 1);
    }

    void clear() {
        memset(m, 0, length >> 3);
        size = 0;
    }

    bitmap *OR(bitmap *b) {
        bitmap *result = new bitmap(length);
        for (int i = 0; i < (int)length >> 5; i++) {
            result -> m[i] = m[i] | b -> m[i];
            result -> size += __builtin_popcount(result -> m[i]);
        }
        return result;
    }

    bitmap *AND(bitmap *b) {
        bitmap *result = new bitmap(length);
        for (int i = 0; i < (int)length >> 5; i++) {
            result -> m[i] = m[i] & b -> m[i];
            result -> size += __builtin_popcount(result -> m[i]);
        }
        return result;
    }

    bitmap *NOT() {
        bitmap *result = new bitmap(length);
        for (int i = 0; i < (int)length >> 5; i++) {
            result -> m[i] = ~m[i];
            result -> size += __builtin_popcount(result -> m[i]);
        }
        return result;
    }

    std::ostringstream print() {
        std::ostringstream oss;
        oss << "[ ";
        for (uint32_t i = 0; i < length; i++) {
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

    void refresh() {
        size = 0;
        for (int i = 0; i < (int)length >> 5; i++) {
            size += __builtin_popcount(m[i]);
        }
    }

};

#endif