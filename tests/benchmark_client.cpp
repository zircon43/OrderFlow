#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <vector>
#include <chrono>
#include <thread>
#include <atomic>
#include <algorithm>
#include <numeric>
#include <cstring>
#include <mutex>
#include "../src/core/types.h"

int NUM_THREADS = 4;
int ORDERS_PER_THREAD = 2500;
int TOTAL_ORDERS = NUM_THREADS * ORDERS_PER_THREAD;
std::atomic<int> trades_received{0};
std::vector<uint64_t> latencies;
std::mutex lat_mutex;

void producer_thread(int thread_id) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(3000);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Producer " << thread_id << " Connection Failed" << std::endl;
        return;
    }

    for (int i = 0; i < ORDERS_PER_THREAD; ++i) {
        Order o;
        o.order_id = (thread_id * 100000) + i + 1;
        o.price = 15000;
        o.quantity = 10;
        o.side = (i % 2 == 0) ? Side::BUY : Side::SELL;
        o.type = OrderType::LIMIT;

        send(sock, &o, sizeof(Order), 0);
    }
    close(sock);
}

void consumer_thread() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(3001); // PubSub port
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    // Give the server a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Consumer Connection Failed" << std::endl;
        return;
    }

    Trade t;
    int expected_trades = TOTAL_ORDERS / 2;
    
    // Set recv timeout to 2 seconds
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    while (trades_received < expected_trades) {
        ssize_t n = recv(sock, &t, sizeof(Trade), 0);
        if (n == sizeof(Trade)) {
            auto now = std::chrono::high_resolution_clock::now();
            uint64_t current_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                now.time_since_epoch()).count();
            
            uint64_t e2e_latency = current_ts - t.taker_ingestion_ts;
            
            std::lock_guard<std::mutex> lock(lat_mutex);
            latencies.push_back(e2e_latency);
            trades_received++;
        } else {
            // Timeout or error
            break;
        }
    }
    close(sock);
}

int main() {
    std::cout << "Starting Concurrent E2E Benchmark..." << std::endl;
    std::cout << "Threads: " << NUM_THREADS << " | Orders per thread: " << ORDERS_PER_THREAD << std::endl;

    // Start consumer
    std::thread consumer(consumer_thread);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Start producers
    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> producers;
    for (int i = 0; i < NUM_THREADS; ++i) {
        producers.push_back(std::thread(producer_thread, i));
    }

    for (auto& t : producers) t.join();
    
    consumer.join();
    auto end_time = std::chrono::high_resolution_clock::now();
    
    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    
    std::cout << "\n--- E2E Benchmark Results ---" << std::endl;
    std::cout << "Total Orders Sent: " << TOTAL_ORDERS << std::endl;
    std::cout << "Total Trades Matched: " << trades_received << std::endl;
    std::cout << "True E2E Throughput: " << (TOTAL_ORDERS / (elapsed_us / 1000000.0)) << " orders/sec" << std::endl;

    if (!latencies.empty()) {
        std::sort(latencies.begin(), latencies.end());
        uint64_t p50 = latencies[latencies.size() * 0.50];
        uint64_t p99 = latencies[latencies.size() * 0.99];
        
        std::cout << "End-to-End Latency p50: " << p50 << " microseconds" << std::endl;
        std::cout << "End-to-End Latency p99: " << p99 << " microseconds" << std::endl;
    }
    
    return 0;
}
