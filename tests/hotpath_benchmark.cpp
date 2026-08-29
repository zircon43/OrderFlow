#include <iostream>
#include <vector>
#include <algorithm>
#include <x86intrin.h>
#include "../src/core/lob.h"
#include "../src/core/ring_buffer.h"
#include <memory>

using TradeBuffer = RingBuffer<Trade, 131072>;

int main() {
    std::cout << "--- Hot-Path Microsecond Benchmark (__rdtscp) ---" << std::endl;
    
    LimitOrderBook lob;
    auto order_queue = std::make_unique<RingBuffer<Order, 131072>>();
    auto trade_queue = std::make_unique<TradeBuffer>();
    
    int NUM_ORDERS = 100000;
    std::vector<Order> preloaded_orders(NUM_ORDERS);
    
    for (int i = 0; i < NUM_ORDERS; ++i) {
        preloaded_orders[i].order_id = i + 1;
        preloaded_orders[i].price = 15000;
        preloaded_orders[i].quantity = 10;
        preloaded_orders[i].side = (i % 2 == 0) ? Side::BUY : Side::SELL;
        preloaded_orders[i].type = OrderType::LIMIT;
        order_queue->push(preloaded_orders[i]);
    }
    
    std::vector<uint64_t> latencies_cycles;
    latencies_cycles.reserve(NUM_ORDERS);
    
    Order current_order;
    unsigned int ui;
    
    // Warmup the CPU caches to avoid cold-start penalty
    for(int i=0; i<1000; ++i) {
        Order warmup{999999+i, 1000, 1, Side::BUY, OrderType::LIMIT};
        lob.process_order(warmup);
    }

    std::cout << "Executing " << NUM_ORDERS << " orders..." << std::endl;
    
    while (order_queue->pop(current_order)) {
        // --- HOT PATH START ---
        uint64_t start = __rdtscp(&ui);
        
        std::vector<Trade> trades = lob.process_order(current_order);
        for (const auto& t : trades) {
            trade_queue->push(t);
        }
        
        uint64_t end = __rdtscp(&ui);
        // --- HOT PATH END ---
        
        latencies_cycles.push_back(end - start);
    }
    
    std::sort(latencies_cycles.begin(), latencies_cycles.end());
    
    // Assuming a conservative 2.5 GHz CPU for cycle-to-nanosecond conversion
    // (1 cycle = 0.4 nanoseconds) -> divide cycles by 2.5 to get nanoseconds
    double cpu_ghz = 2.5; 
    
    auto print_percentile = [&](const char* name, double pct) {
        uint64_t cycles = latencies_cycles[latencies_cycles.size() * pct];
        double nanoseconds = cycles / cpu_ghz;
        double microseconds = nanoseconds / 1000.0;
        std::cout << name << ": " << cycles << " cycles | " 
                  << nanoseconds << " ns | " 
                  << microseconds << " µs" << std::endl;
    };
    
    std::cout << "\n--- Isolated Matching Latency Results ---" << std::endl;
    print_percentile("p50   ", 0.50);
    print_percentile("p90   ", 0.90);
    print_percentile("p99   ", 0.99);
    print_percentile("p99.9 ", 0.999);
    print_percentile("MAX   ", 0.99999);
    
    return 0;
}
