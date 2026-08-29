#pragma once
#include "types.h"
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

class KafkaProducer {
public:
    KafkaProducer(const std::string& host, int port, const std::string& topic);
    ~KafkaProducer();

    void start();
    void stop();
    
    // Non-blocking produce
    void produce_trade(const Trade& trade);

private:
    void background_loop();
    bool connect_broker();
    void send_produce_request(const std::vector<Trade>& batch);

    std::string host_;
    int port_;
    std::string topic_;
    int sock_;
    int32_t correlation_id_;
    
    std::atomic<bool> running_;
    std::thread worker_;
    
    std::vector<Trade> queue_;
    std::mutex mu_;
    std::condition_variable cv_;
};