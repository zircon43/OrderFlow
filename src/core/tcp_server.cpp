#include "tcp_server.h"
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <chrono>
#include <unordered_set>
#include <deque>
#include <mutex>

#define MAX_EVENTS 1024

// Set socket to non-blocking
void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

TcpServer::TcpServer(MatchingEngine* engine, int port) 
    : engine_(engine), port_(port), server_fd_(-1), epoll_fd_(-1) {}

TcpServer::~TcpServer() {
    stop();
}

void TcpServer::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == -1) {
        throw std::runtime_error("Failed to create socket");
    }

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        throw std::runtime_error("Failed to bind socket");
    }

    if (listen(server_fd_, SOMAXCONN) == -1) {
        throw std::runtime_error("Failed to listen on socket");
    }

    set_nonblocking(server_fd_);

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ == -1) {
        throw std::runtime_error("Failed to create epoll fd");
    }

    epoll_event event{};
    event.events = EPOLLIN | EPOLLET; // Edge-triggered
    event.data.fd = server_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &event) == -1) {
        throw std::runtime_error("Failed to add server_fd to epoll");
    }

    running_ = true;
    worker_thread_ = std::thread(&TcpServer::epoll_loop, this);
    
    std::cout << "[TCP Server] Listening for binary orders on port " << port_ << std::endl;
}

void TcpServer::stop() {
    if (running_) {
        running_ = false;
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        if (server_fd_ != -1) close(server_fd_);
        if (epoll_fd_ != -1) close(epoll_fd_);
    }
}

void TcpServer::epoll_loop() {
    epoll_event events[MAX_EVENTS];
    char buffer[sizeof(Order)];
    
    // Idempotency set with O(1) bounded memory eviction policy
    std::unordered_set<uint64_t> seen_orders;
    std::deque<uint64_t> order_history;
    const size_t MAX_IDEMPOTENCY_CACHE = 100000;
    
    while (running_) {
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);
        
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == server_fd_) {
                // New connection
                while (true) {
                    sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
                    if (client_fd == -1) {
                        break;
                    }
                    set_nonblocking(client_fd);
                    epoll_event evt{};
                    evt.events = EPOLLIN | EPOLLET;
                    evt.data.fd = client_fd;
                    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &evt);
                }
            } else {
                // Read data from client
                int client_fd = events[i].data.fd;
                while (true) {
                    ssize_t bytes_read = recv(client_fd, buffer, sizeof(Order), 0);
                    if (bytes_read == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break; // Done reading
                        }
                        // Error
                        close(client_fd);
                        break;
                    } else if (bytes_read == 0) {
                        // Client disconnected
                        close(client_fd);
                        break;
                    } else if (bytes_read == sizeof(Order)) {
                        auto now = std::chrono::high_resolution_clock::now();
                        uint64_t ts = std::chrono::duration_cast<std::chrono::microseconds>(
                            now.time_since_epoch()).count();

                        Order* order = reinterpret_cast<Order*>(buffer);
                        
                        // Idempotency Check with Eviction
                        if (seen_orders.find(order->order_id) == seen_orders.end()) {
                            if (seen_orders.size() >= MAX_IDEMPOTENCY_CACHE) {
                                uint64_t oldest = order_history.front();
                                order_history.pop_front();
                                seen_orders.erase(oldest);
                            }
                            seen_orders.insert(order->order_id);
                            order_history.push_back(order->order_id);
                            
                            order->ingestion_ts = ts;
                            engine_->submit_order(*order);
                        }
                    } else {
                        // For simplicity in this high-performance prototype, we assume clients 
                        // send exact 22-byte boundaries over TCP (no fragmentation handling). 
                        // In production, we would buffer partial reads.
                    }
                }
            }
        }
    }
}
