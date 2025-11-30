#include <atomic>
#include <csignal>
#include <iostream>
#include <thread>
#include <vector>

#include <broker.hpp>
#include <common.hpp>

std::atomic<bool> g_running(true);

void handler(int) {
    g_running = false;
    std::cout << "\n[MAIN] Caught signal, exiting...\n";
}

int main(int argc, char** argv) {
    std::signal(SIGINT, handler);
    std::signal(SIGTERM, handler);

    // Default port if not provided
    std::string bindPort = "tcp://127.0.0.1:7001";
    
    // Allow passing port as arg: ./broker tcp://*:7002
    if (argc > 1) {
        bindPort = "tcp://127.0.0.1:" + std::string(argv[1]);
    }

    const std::string config_path = "config/broker.conf";
    std::string heartBeatEndpoint;
    if (!read_one_line(config_path, heartBeatEndpoint)) {
        heartBeatEndpoint = "tcp://127.0.0.1:6001";
    }

    Broker broker(bindPort, heartBeatEndpoint);

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