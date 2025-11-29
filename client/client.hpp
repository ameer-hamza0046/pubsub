#pragma once
#include <atomic>
#include <string>
#include <zmq.hpp>

#include "../../common/common.hpp"

class Client {
   public:
    Client(const std::string& endpoint);
    ~Client();

    void close();

    bool publish(const std::string& topic, const std::string& msg);
    bool get_latest_message_id(const std::string& topic, int& message_idOut);
    bool get_message_by_id(const std::string& topic, int message_id,
                           std::string& messageOut);

   private:
    zmq::context_t ctx;
    zmq::socket_t socket;
    std::string endpoint;
    std::atomic<bool> closed{false};
};
