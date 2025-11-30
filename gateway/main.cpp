#include <atomic>
#include <common.hpp>
#include <csignal>
#include <gateway.hpp>
#include <iostream>
#include <thread>

std::atomic<bool> g_running(true);

void handler(int) {
    g_running = false;
    std::cout << "\n[MAIN] Caught signal, exiting...\n";
}

int main() {
    std::signal(SIGINT, handler);
    std::signal(SIGTERM, handler);

    const std::string config_file = "config/gateway.conf";

    std::vector<std::string> lines;
    if (!read_all_lines(config_file, lines) or lines.size() < 3) {
        std::cerr << "[MAIN] Warning: could not read config file: "
                  << config_file << "\n";
        return 1;
    }

    // listenAddr: where clients connect
    // brokerAddr: where the dedicated broker process is listening
    auto const listenAddr = lines[0];
    auto const heartBeatPort = stoi(lines[1]);
    lines.erase(lines.begin(), lines.begin() + 2); // Remove first two lines

    Gateway gw(listenAddr, lines, heartBeatPort);

    // Run in a thread or main loop
    std::thread gw_thread([&gw]() { gw.run(); });

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "[Main] Stopping gateway...\n";
    gw.close();
    gw_thread.join();
    return 0;
}
