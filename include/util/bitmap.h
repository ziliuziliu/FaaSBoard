#ifndef _BITMAP_H
#define _BITMAP_H

#include <cstdint>
#include <cstring>

// TODO: operator support bitmaps with different length

struct bitmap {
    
    uint32_t size, length;
    uint32_t *m;

    bitmap() {}

    bitmap(uint32_t length) {
        this -> size = 0;
        this -> length = ((length >> 5) + 1) << 5;
        this -> m = new uint32_t[this -> length >> 5]();
    }

    ~bitmap() {
        delete [] m;
    }

    bool exist(uint32_t x) {
        uint32_t slot = x >> 5, bit = x & 0x1f;
        return m[slot] & (1 << bit);
    }

    void add(uint32_t x) {
        if (!exist(x)) {
            uint32_t slot = x >> 5, bit = x & 0x1f;
            m[slot] |= (1 << bit);
            size++;
        }
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

    void clear() {
        memset(m, 0, length >> 3);
        size = 0;
    }

};

#endif