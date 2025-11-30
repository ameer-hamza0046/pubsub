#pragma once

#include <atomic>
#include <common.hpp>
#include <lru_cache.hpp>
#include <string>
#include <thread>
#include <vector>
#include <zmq.hpp>

class Broker {
   public:
    Broker(const std::string& bindAddr, const std::string& gatewayHbAddr,
           const std::string& dbNodeAddr);
    ~Broker();

    void run();
    void close();

   private:
    zmq::context_t ctx;
    zmq::socket_t socket;     // ROUTER (Clients/Gateway)
    zmq::socket_t db_socket;  // REQ (Talks to DB Node)

    std::string bindAddr;
    std::atomic<bool> stopRequested{false};

    // Heartbeat Thread
    std::thread heartbeat_thread;
    void heartbeat_routine(std::string gatewayHbAddr, std::string myAddr);

    // --- CACHES ---
    // 1. Latest ID Cache: Topic -> String(ID)
    LRUCache<std::string, std::string> latest_id_cache;

    // 2. Message Cache: "Topic;ID" -> MessageBody
    LRUCache<std::string, std::string> msg_cache;

    // Helpers
    bool receive_multipart(std::vector<zmq::message_t>& out_parts);
    bool send_multipart(std::vector<zmq::message_t>& parts);

    // Core Logic
    std::string process_command(const std::string& payload);

    // DB Helpers
    std::string query_db(const std::string& requestStr);
};