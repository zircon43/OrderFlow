#include "pubsub_server.h"
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>

#define MAX_EVENTS 100

// Helper to set non-blocking
static void set_nonblocking_pub(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

PubSubServer::PubSubServer(MatchingEngine* engine, int port)
    : engine_(engine), port_(port), server_fd_(-1), epoll_fd_(-1) {}

PubSubServer::~PubSubServer() {
    stop();
}

void PubSubServer::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd_, SOMAXCONN);
    set_nonblocking_pub(server_fd_);

    epoll_fd_ = epoll_create1(0);
    epoll_event event{};
    event.events = EPOLLIN | EPOLLET;
    event.data.fd = server_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &event);

    running_ = true;
    accept_thread_ = std::thread(&PubSubServer::accept_loop, this);
    broadcast_thread_ = std::thread(&PubSubServer::broadcast_loop, this);
    
    std::cout << "[PubSub] Listening for telemetry subscribers on port " << port_ << std::endl;
}

void PubSubServer::stop() {
    running_ = false;
    if (accept_thread_.joinable()) accept_thread_.join();
    if (broadcast_thread_.joinable()) broadcast_thread_.join();
    if (server_fd_ != -1) close(server_fd_);
    if (epoll_fd_ != -1) close(epoll_fd_);
}

void PubSubServer::accept_loop() {
    epoll_event events[MAX_EVENTS];
    while (running_) {
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == server_fd_) {
                while (true) {
                    sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
                    if (client_fd == -1) break;
                    
                    set_nonblocking_pub(client_fd);
                    std::lock_guard<std::mutex> lock(subs_mutex_);
                    subscribers_[client_fd] = Subscriber{client_fd, {}, false};
                }
            } else {
                // If we get an event on a client socket, they either disconnected or sent junk.
                // We just close the connection.
                int client_fd = events[i].data.fd;
                std::lock_guard<std::mutex> lock(subs_mutex_);
                subscribers_.erase(client_fd);
                close(client_fd);
            }
        }
    }
}

void PubSubServer::broadcast_loop() {
    std::vector<Trade> trades;
    while (running_) {
        trades.clear();
        if (engine_->poll_trades(trades) && !trades.empty()) {
            std::lock_guard<std::mutex> lock(subs_mutex_);
            for (auto& [fd, sub] : subscribers_) {
                for (const auto& t : trades) {
                    if (sub.buffer.size() >= MAX_BUFFER_SIZE) {
                        sub.buffer.pop_front(); // Drop oldest! (Pub-Sub isolation)
                    }
                    sub.buffer.push_back(t);
                }
                drain_subscriber(fd, sub);
            }
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}

void PubSubServer::drain_subscriber(int fd, Subscriber& sub) {
    if (sub.is_sending || sub.buffer.empty()) return;
    
    sub.is_sending = true;
    while (!sub.buffer.empty()) {
        const Trade& t = sub.buffer.front();
        ssize_t bytes_sent = send(fd, &t, sizeof(Trade), MSG_NOSIGNAL);
        
        if (bytes_sent == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Socket full, stop sending, buffer remains
                break; 
            } else {
                // Error (client disconnected)
                // We'll let the accept_loop clean it up when it gets the EPOLLERR/HUP
                break;
            }
        } else if (bytes_sent == sizeof(Trade)) {
            sub.buffer.pop_front();
        } else {
            // Partial send logic would go here for robust production
            break;
        }
    }
    sub.is_sending = false;
}
