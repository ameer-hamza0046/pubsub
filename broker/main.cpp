#include <atomic>
#include <broker.hpp>
#include <common.hpp>
#include <csignal>
#include <iostream>
#include <thread>
#include <vector>

std::atomic<bool> g_running(true);

void handler(int) {
    g_running = false;
    std::cout << "\n[MAIN] Caught signal, exiting...\n";
}

int main(int argc, char** argv) {
    std::signal(SIGINT, handler);
    std::signal(SIGTERM, handler);

    // 1. Broker Data Port (Default 7001)
    std::string bindPort = "tcp://127.0.0.1:7001";
    if (argc > 1) {
        std::string arg = argv[1];
        bindPort = "tcp://127.0.0.1:" + arg;
    }

    // 2. Load Configs
    const std::string config_path = "config/broker.conf";
    std::string gatewayHbEndpoint;

    std::vector<std::string> lines;
    if (!read_all_lines(config_path, lines)) {
        std::cerr << "[MAIN] Could not read config file: " << config_path
                  << ", using defaults.\n";
    }
    // Default Heartbeat (Gateway)
    if (lines.size() >= 1) {
        gatewayHbEndpoint = lines[0];
    }

    // 3. DB Node Address (Fixed for now, or could act as arg)
    // The DB Node listens on 8000 (REP)
    std::string dbNodeEndpoint =
        lines.size() >= 2 ? lines[1] : "tcp://127.0.0.1:8000";

    std::cout << "--- Broker Configuration ---\n"
              << "Listen Addr:   " << bindPort << "\n"
              << "Gateway HB:    " << gatewayHbEndpoint << "\n"
              << "DB Node:       " << dbNodeEndpoint << "\n"
              << "----------------------------\n";

    Broker broker(bindPort, gatewayHbEndpoint, dbNodeEndpoint);

    std::thread broker_thread([&broker]() { broker.run(); });

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    broker.close();
    broker_thread.join();

    return 0;
}