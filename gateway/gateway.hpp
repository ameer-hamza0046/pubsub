#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <zmq.hpp>

// Track the state of each broker
struct BackendNode {
    std::string address;
    bool active;
    std::chrono::steady_clock::time_point last_seen;
};

class Gateway {
   public:
    Gateway(const std::string& listenAddr,
            const std::vector<std::string>& brokers,
            int heartbeatPort = 6002);  // New arg
    ~Gateway();

    void run();
    void close();

   private:
    zmq::context_t ctx;

    zmq::socket_t frontend;               // ROUTER
    std::vector<zmq::socket_t> backends;  // DEALERs

    // --- FAULT TOLERANCE MEMBERS ---
    zmq::socket_t heartbeat_puller;  // PULL socket
    std::thread heartbeat_thread;
    std::mutex registry_mutex;  // Protects access to node_states

    // Parallel vector to 'backends'. Index i in backends matches index i here.
    std::vector<BackendNode> node_states;
    // Helper to find index by address
    std::map<std::string, int> address_to_index;
    // -------------------------------

    std::string listenAddr;
    std::vector<std::string> brokerAddrs;

    std::atomic<bool> stopRequested{false};

    // Modified to return -1 if no brokers available
    int choose_backend_index(const std::vector<zmq::message_t>& parts);

    // Background listener
    void heartbeat_listener();

    // Helpers
    bool receive_multipart(zmq::socket_t& sock,
                           std::vector<zmq::message_t>& out_parts);
    bool send_multipart(zmq::socket_t& sock,
                        std::vector<zmq::message_t>& parts);
};