#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>
#include <thread>
#include <zmq.hpp>

#include <common.hpp>

class Broker {
   public:
    // Updated constructor to accept Gateway Heartbeat Address
    Broker(const std::string& bindAddr, const std::string& gatewayHbAddr);
    ~Broker();

    void run();
    void close();

   private:
    zmq::context_t ctx;
    zmq::socket_t socket; // ROUTER
    std::string bindAddr;
    std::atomic<bool> stopRequested{false};

    // Heartbeat Thread
    std::thread heartbeat_thread;
    void heartbeat_routine(std::string gatewayHbAddr, std::string myAddr);

    // In-memory storage: Topic -> List of Messages
    std::map<std::string, std::vector<std::string>> storage;

    // Helpers
    bool receive_multipart(std::vector<zmq::message_t>& out_parts);
    bool send_multipart(std::vector<zmq::message_t>& parts);
    
    // Business Logic
    std::string process_command(const std::string& payload);
};