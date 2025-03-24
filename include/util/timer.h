#ifndef _TIMER_H
#define _TIMER_H

#include "util/log.h"

#include <sys/time.h>

inline double get_time() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_sec + (tv.tv_usec / 1e6);
}

class timer {

public:
    double start_time, tick_time;
    std::string tick_info;
    
    timer() {
        start_time = tick_time = 0.0;
        tick_info = "";
    }

    void start() {
        start_time = get_time();
        VLOG(1) << "====== start ======";
    }
    
    double from_start(std::string info) {
        double interval = get_time() - start_time;
        VLOG(1) << "====== from start " << info << " " << interval << "s ======";
        return interval;
    }

    void tick(std::string info) {
        tick_time = get_time();
        tick_info = info;
        VLOG(1) << "====== tick " << info << " ======";
    }
    
    double from_tick() {
        double interval = get_time() - tick_time;
        VLOG(1) << "====== from tick " << tick_info << " " << interval << "s ======";
        return interval;
    }

};

#endif