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

void test_publish_mode(Client& client) {
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
}

void test_retrieve_mode(Client& client) {
    const std::string topic = "greetings";
    while (running) {
        int latest_id = -1;
        if (!client.get_latest_message_id(topic, latest_id)) {
            std::cout << "[Client] Failed to get latest message ID\n";
            break;
        }
        std::cout << "[Client] Latest message ID for topic " << topic << ": "
                  << latest_id << "\n";

        if (latest_id >= 0) {
            std::string message;
            if (!client.get_message_by_id(topic, latest_id, message)) {
                std::cout << "[Client] Failed to get message by ID\n";
                break;
            }
            std::cout << "[Client] Retrieved message: " << message << "\n";
        }

        std::this_thread::sleep_for(std::chrono::seconds(3));
    }
}

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    const std::string gatewayEndpoint = "tcp://127.0.0.1:6000";

    Client client(gatewayEndpoint);

    // test_publish_mode(client);
    test_retrieve_mode(client);

    client.close();
    std::cout << "Exited cleanly.\n";
    return 0;
}
