#include "engine.h"
#include <iostream>
#include <pthread.h>
#include <sched.h>

MatchingEngine::MatchingEngine() {}

MatchingEngine::~MatchingEngine() {
    stop();
}

void MatchingEngine::start() {
    if (!running.exchange(true)) {
        worker_thread = std::thread(&MatchingEngine::loop, this);
    }
}

void MatchingEngine::stop() {
    if (running.exchange(false)) {
        if (worker_thread.joinable()) {
            worker_thread.join();
        }
    }
}

bool MatchingEngine::submit_order(const Order& order) {
    return order_queue.push(order);
}

bool MatchingEngine::poll_trades(std::vector<Trade>& out_trades) {
    Trade t;
    bool has_trades = false;
    // Drain up to 1000 trades at a time to prevent blocking Node.js event loop
    int count = 0;
    while (count < 1000 && trade_queue.pop(t)) {
        out_trades.push_back(t);
        has_trades = true;
        count++;
    }
    return has_trades;
}

void MatchingEngine::loop() {
    // OS-level Tuning: Pin the matching thread to Core 1 (or another isolated core)
    #if defined(__linux__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(1, &cpuset); // Pin to core 1
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    std::cout << "[C++ Engine] Matching thread pinned to CPU Core 1" << std::endl;
    #endif

    Order current_order;
    while (running.load(std::memory_order_relaxed)) {
        if (order_queue.pop(current_order)) {
            // Process order in the LOB
            std::vector<Trade> trades = lob.process_order(current_order);
            
            // Push trades to the output queue
            for (const auto& t : trades) {
                // If queue is full, we lose trades in this simple implementation.
                // In production, we'd spin until there's space.
                while (!trade_queue.push(t) && running.load(std::memory_order_relaxed)) {
                     #if defined(__x86_64__)
                        __asm__ volatile("pause\n": : :"memory");
                     #endif
                }
            }
        } else {
            // Yield briefly
            #if defined(__x86_64__)
                __asm__ volatile("pause\n": : :"memory");
            #endif
        }
    }
}
