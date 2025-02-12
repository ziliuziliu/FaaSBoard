#ifndef MATH_TOOLS_H
#define MATH_TOOLS_H

#include <cmath>
#include <utility>

// n >= 1
std::pair<int, int> factorizeInt(int n) {
    if (n == 1) {
        return {1, 1};
    }

    for (int i = (int)sqrt(n); i >= 2; --i) {
        if (n % i == 0) {
            return {i, n / i};
        }
    }
    return {1, n};
}

#endif