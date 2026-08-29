#pragma once

#include "engine.h"
#include <atomic>
#include <thread>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <deque>

// Subscriber state with a bounded buffer
struct Subscriber {
    int fd;
    std::deque<Trade> buffer;
    bool is_sending = false;
};

class PubSubServer {
public:
    PubSubServer(MatchingEngine* engine, int port);
    ~PubSubServer();

    void start();
    void stop();

private:
    void accept_loop();
    void broadcast_loop();
    void drain_subscriber(int fd, Subscriber& sub);

    MatchingEngine* engine_;
    int port_;
    int server_fd_;
    int epoll_fd_;
    std::atomic<bool> running_{false};
    
    std::thread accept_thread_;
    std::thread broadcast_thread_;
    
    std::mutex subs_mutex_;
    std::unordered_map<int, Subscriber> subscribers_;
    
    const size_t MAX_BUFFER_SIZE = 1000; // Drop-oldest policy limit
};
