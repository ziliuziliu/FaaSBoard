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
    static double start_time, tick_time;
    static std::string tick_info;
    
    static void start() {
        start_time = get_time();
        VLOG(1) << "====== start ======";
    }
    
    static void from_start(std::string info) {
        double interval = get_time() - start_time;
        VLOG(1) << "====== from start " << info << " " << interval << "s ======";
    }

    static void tick(std::string info) {
        tick_time = get_time();
        tick_info = info;
        VLOG(1) << "====== tick " << info << " ======";
    }
    
    static void from_tick() {
        double interval = get_time() - tick_time;
        VLOG(1) << "====== from tick " << tick_info << " " << interval << "s ======";
    }

};

double timer::start_time = 0.0;
double timer::tick_time = 0.0;
std::string timer::tick_info = "";

#endif