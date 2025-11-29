#include <atomic>
#include <csignal>
#include <iostream>

#include "gateway.hpp"

std::atomic<bool> running(true);

void signal_handler(int) {
    running = false;
    std::cout << "\nCtrl+C detected, stopping server...\n";
}

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Multithreaded Server (Gateway)
    Gateway server("tcp://*:5555", 4);

    std::cout << "Press Ctrl+C to exit.\n";

    // Start in-blocking mode, exits only via proxy break (Ctrl+C)
    server.start();  // blocking

    return 0;
}
