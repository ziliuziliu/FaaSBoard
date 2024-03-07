#ifndef _PRINT_H
#define _PRINT_H

#include <iostream>
#include <vector>
#include <cstring>
#include <chrono>

auto previous = std::chrono::high_resolution_clock::now();

template <class T>
void print_vector(std::vector<T> vec) {
    std::cout << "[ ";
    for (auto v: vec)
        std::cout << v << ", ";
    std::cout << "]";
}

template <class T>
void print_array(T *arr, int length) {
    std::cout << "[ ";
    for (int i = 0; i < length; i++)
        std::cout << arr[i] << ", ";
    std::cout << "]";
}

void print_log(std::string s) {
    auto current = std::chrono::high_resolution_clock::now();
    double time = std::chrono::duration_cast<std::chrono::nanoseconds>(current - previous).count();
    time *= 1e-9;
    std::cout << "====== Time: " << time << "s, " << s << " ======" << std::endl;
}

#endif