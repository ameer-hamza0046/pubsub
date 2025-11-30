#include <atomic>
#include <common.hpp>
#include <csignal>
#include <disk_node.hpp>
#include <iostream>
#include <string>
#include <vector>
#include <zmq.hpp>

std::atomic<bool> g_running(true);

void handler(int) { g_running = false; }

int main() {
    std::signal(SIGINT, handler);
    std::signal(SIGTERM, handler);

    zmq::context_t ctx(1);
    // REP socket because we must reply to the Broker
    zmq::socket_t socket(ctx, ZMQ_REP);

    std::string bindAddr = "tcp://*:8000";
    std::string disk_cfg = "config/db_node.cfg";
    std::string storage_file = "storage.db";

    if (!read_one_line(disk_cfg, bindAddr)) {
        std::cout << "[DB Node] Using default bind address " << bindAddr
                  << "\n";
    }

    std::cout << "[DB Node] Listening on " << bindAddr << "\n";
    socket.bind(bindAddr);

    SimpleDB db(storage_file);

    while (g_running) {
        zmq::message_t request;
        // Poll to allow exit
        zmq::pollitem_t items[] = {
            {static_cast<void*>(socket), 0, ZMQ_POLLIN, 0}};
        zmq::poll(items, 1, std::chrono::milliseconds(200));

        if (items[0].revents & ZMQ_POLLIN) {
            auto ret = socket.recv(request, zmq::recv_flags::none);
            if (!ret.has_value()) {
                std::cout << "[DB Node] Receive error\n";
            }
            std::string reqStr(static_cast<char*>(request.data()),
                               request.size());

            // Parse Command
            auto parts = split(reqStr, DELIM);
            std::string respStr = RESP_ERR;

            if (!parts.empty()) {
                std::string cmd = parts[0];

                // 1. INSERT;TOPIC;MSG
                if (cmd == CMD_PUBLISH && parts.size() >= 3) {
                    std::string topic = parts[1];
                    std::string msg = parts[2];

                    int new_id = db.insert(topic, msg);
                    // Return ACK;NEW_ID
                    respStr = RESP_ACK + DELIM + std::to_string(new_id);
                    std::cout << "[DB] Inserted " << topic << ":" << new_id
                              << "\n";
                }
                // 2. GET_LATEST;TOPIC
                else if (cmd == CMD_GET_LATEST_ID && parts.size() >= 2) {
                    std::string topic = parts[1];
                    int id = db.get_latest_id(topic);
                    respStr = std::to_string(id);  // Just return the number
                }
                // 3. GET_MSG;TOPIC;ID
                else if (cmd == CMD_GET_MESSAGE_BY_ID && parts.size() >= 3) {
                    std::string topic = parts[1];
                    try {
                        int id = std::stoi(parts[2]);
                        std::string msg = db.get_message(topic, id);
                        if (!msg.empty()) {
                            respStr = msg;
                        } else {
                            respStr = RESP_ERR;
                        }
                    } catch (...) {
                    }
                }
            }

            // Send Reply
            zmq::message_t reply(respStr.data(), respStr.size());
            socket.send(reply, zmq::send_flags::none);
        }
    }

    return 0;
}