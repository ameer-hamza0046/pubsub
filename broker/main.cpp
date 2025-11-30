#include <atomic>
#include <csignal>
#include <iostream>
#include <thread>
#include <vector>

#include "broker.hpp"

std::atomic<bool> g_running(true);

void handler(int) {
    g_running = false;
    std::cout << "\n[MAIN] Caught signal, exiting...\n";
}

int main(int argc, char** argv) {
    std::signal(SIGINT, handler);
    std::signal(SIGTERM, handler);

    // Default port if not provided
    std::string bindPort = "tcp://*:6001";
    
    // Allow passing port as arg: ./broker tcp://*:6002
    if (argc > 1) {
        bindPort = "tcp://*:" + std::string(argv[1]);
    }

    Broker broker(bindPort);

    // Run in thread
    std::thread broker_thread([&broker](){
        broker.run();
    });

    while(g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    broker.close();
    broker_thread.join();

    return 0;
}