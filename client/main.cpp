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

    const std::string gatewayEndpoint = "tcp://127.0.0.1:6000";

    Client client(gatewayEndpoint);

    int counter = 0;

    while (running) {
        std::string topic = "greetings";
        std::string msg = "hello-" + std::to_string(counter++);
        // client.send(msg);
        std::cout << "[Client] Publishing message: " << msg
                  << " to topic: " << topic << std::endl;
        auto res = client.publish(topic, msg);
        if (!res) {
            std::cout << "[Client] Publish failed\n";
            break;
        }
        std::cout << "[Client] Published successfully\n";
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    client.close();
    std::cout << "Exited cleanly.\n";
    return 0;
}
