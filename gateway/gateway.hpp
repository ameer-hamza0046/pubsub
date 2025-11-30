#pragma once

#include <atomic>
#include <string>
#include <vector>
#include <zmq.hpp>

class Gateway {
   public:
    Gateway(const std::string& listenAddr,
            const std::vector<std::string>& brokers);
    ~Gateway();

    // The main non-blocking loop
    void run();

    // Signal the loop to stop
    void close();

   private:
    zmq::context_t ctx;

    // Frontend: Clients connect here (ROUTER)
    // We use ROUTER so we get the identity of the client
    zmq::socket_t frontend;

    // Backend: One DEALER socket per Broker
    // We use a vector of sockets so we can pick exactly which one to send to
    std::vector<zmq::socket_t> backends;

    std::string listenAddr;
    std::vector<std::string> brokerAddrs;

    std::atomic<bool> stopRequested{false};

    // Logic to parse the message and pick a broker index (0 to N-1)
    size_t choose_backend_index(const std::vector<zmq::message_t>& parts);

    // Helpers to keep code clean
    bool receive_multipart(zmq::socket_t& sock,
                           std::vector<zmq::message_t>& out_parts);
    bool send_multipart(zmq::socket_t& sock,
                        std::vector<zmq::message_t>& parts);
};