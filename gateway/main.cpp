#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include "gateway.hpp"

std::atomic<bool> g_running(true);

void handler(int) {
    g_running = false;
    std::cout << "\n[MAIN] Caught signal. Shutting down...\n";
}

int main() {
    std::signal(SIGINT, handler);
    std::signal(SIGTERM, handler);

    Gateway gw("tcp://*:6000", 4);
    gw.start();

    std::cout << "[MAIN] Gateway running. Ctrl+C to stop.\n";

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "[MAIN] Main loop exiting, calling gw.stop()...\n";
    gw.stop();

    std::cout << "[MAIN] Exit.\n";
    return 0;
}
