#ifndef _UTILS_H
#define _UTILS_H

#define INCOMING 10000
#define OUTGOING 10001

#include <string>

void parse_line(char *line, int line_size, uint32_t *x, uint32_t *y) {
    *x = *y = 0;
    bool first_num = true;
    for (int i = 0; i < line_size; i++) {
        if (line[i] >= '0' && line[i] <= '9') {
            if (first_num) *x = *x * 10 + line[i] - '0';
            else *y = *y * 10 + line[i] - '0';
        } else {
            if (first_num) first_num = false;
            else break;
        }
    }
}

#endif