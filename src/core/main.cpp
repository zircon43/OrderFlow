#include "engine.h"
#include "tcp_server.h"
#include "pubsub_server.h"
#include <iostream>
#include <csignal>
#include <memory>

std::unique_ptr<MatchingEngine> global_engine;
std::unique_ptr<TcpServer> global_server;
std::unique_ptr<PubSubServer> global_pubsub;

void signal_handler(int signal) {
    if (global_server) {
        std::cout << "\nStopping TCP Server..." << std::endl;
        global_server->stop();
    }
    if (global_pubsub) {
        std::cout << "Stopping PubSub Server..." << std::endl;
        global_pubsub->stop();
    }
    if (global_engine) {
        std::cout << "Stopping Engine..." << std::endl;
        global_engine->stop();
    }
}

int main(int argc, char* argv[]) {
    int port = 3000;
    if (argc >= 2) {
        port = std::stoi(argv[1]);
    }

    try {
        global_engine = std::make_unique<MatchingEngine>();
        global_server = std::make_unique<TcpServer>(global_engine.get(), port);
        global_pubsub = std::make_unique<PubSubServer>(global_engine.get(), port + 1);
        
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        
        global_engine->start(); // Starts the pinned thread for matching
        global_server->start(); // Starts the epoll thread for ingestion
        global_pubsub->start(); // Starts the telemetry broadcast threads

        std::cout << "Engine is running. Press Ctrl+C to stop." << std::endl;

        // Block main thread until signal
        while (global_engine && global_server) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
