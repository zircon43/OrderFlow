#pragma once

#include "lob.h"
#include "ring_buffer.h"
#include <atomic>
#include <thread>
#include <vector>

class MatchingEngine {
public:
    MatchingEngine();
    ~MatchingEngine();

    // Start the background matching thread
    void start();
    
    // Stop the thread
    void stop();

    // Submit an order from Node.js into the LOB
    bool submit_order(const Order& order);

    // Poll for completed trades
    bool poll_trades(std::vector<Trade>& out_trades);

private:
    void loop();

    LimitOrderBook lob;
    std::atomic<bool> running{false};
    std::thread worker_thread;

    // Lock-free queues (capacity 65536)
    RingBuffer<Order, 65536> order_queue;
    RingBuffer<Trade, 65536> trade_queue;
};
