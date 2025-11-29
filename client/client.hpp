#pragma once
#include <atomic>
#include <string>
#include <zmq.hpp>

class Client {
   public:
    Client(const std::string& endpoint);
    ~Client();

    bool send(const std::string& msg);
    bool recv(std::string& reply);

    void close();

   private:
    zmq::context_t ctx;
    zmq::socket_t socket;
    std::string endpoint;
    std::atomic<bool> closed{false};
};
