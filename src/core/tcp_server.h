#pragma once

#include "engine.h"
#include <atomic>
#include <thread>
#include <vector>

class TcpServer {
public:
    TcpServer(MatchingEngine* engine, int port);
    ~TcpServer();

    void start();
    void stop();

private:
    void epoll_loop();

    MatchingEngine* engine_;
    int port_;
    int server_fd_;
    int epoll_fd_;
    std::atomic<bool> running_{false};
    std::thread worker_thread_;
};
