#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include "client.hpp"

std::atomic<bool> running(true);

void signal_handler(int) {
    running = false;
    std::cout << "\nCtrl+C detected, stopping...\n";
}

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    const std::string endpoint = "tcp://127.0.0.1:6001";

    Client client(endpoint);

    int counter = 0;

    while (running) {
        std::string msg = "hello " + std::to_string(counter++);
        client.send(msg);
        std::cout << "[Client] Sent: " << msg << std::endl;

        std::string reply;
        if (client.recv(reply)) {
            std::cout << "[Client] Received: " << reply << std::endl;
        } else {
            std::cout << "[Client] Receive failed\n";
            break;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    client.close();
    std::cout << "Exited cleanly.\n";
    return 0;
}
