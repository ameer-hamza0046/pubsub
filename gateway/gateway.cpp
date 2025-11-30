#include "gateway.hpp"

#include <functional>  // for std::hash
#include <iostream>

#include "../../common/common.hpp"

Gateway::Gateway(const std::string& listenAddr,
                 const std::vector<std::string>& brokers)
    : ctx(1),
      frontend(ctx, ZMQ_ROUTER),
      listenAddr(listenAddr),
      brokerAddrs(brokers) {
    // 1. Setup Frontend
    std::cout << "[Gateway] Binding frontend to " << listenAddr << "\n";
    frontend.bind(listenAddr);

    // 2. Setup Backends
    // We reserve space to prevent vector resizing from moving sockets (which is
    // expensive/tricky)
    backends.reserve(brokers.size());

    for (const auto& addr : brokers) {
        std::cout << "[Gateway] Connecting backend to " << addr << "\n";
        // Create socket in place
        backends.emplace_back(ctx, ZMQ_DEALER);
        // Connect the last created socket
        backends.back().connect(addr);
    }
}

Gateway::~Gateway() { close(); }

void Gateway::close() { stopRequested = true; }

size_t Gateway::choose_backend_index(const std::vector<zmq::message_t>& parts) {
    // Structure of a ROUTER message:
    // [0] = Client Identity
    // [1] = Empty Frame (usually, if using REQ client)
    // [2] = Topic / Command / Data

    if (parts.size() < 3) return 0;  // Fallback

    // Let's interpret the 3rd frame (index 2) as the "Key" for routing
    const auto& payload_frame = parts[2];
    std::string key(static_cast<const char*>(payload_frame.data()),
                    payload_frame.size());

    auto parsed = split(key, DELIM);
    // parsed[0] is CMD
    // parsed[1] is TOPIC
    key = parsed[1];

    // EXAMPLE: Hash based routing
    // If key is "UserA", it might go to broker 0. "UserB" to broker 1.
    size_t hash = std::hash<std::string>{}(key);

    // Modulo arithmetic to pick a valid index
    return hash % backends.size();
}

void Gateway::run() {
    // We need a poll item for the frontend + one for each backend
    std::vector<zmq::pollitem_t> items;

    // Item 0: Frontend
    items.push_back({static_cast<void*>(frontend), 0, ZMQ_POLLIN, 0});

    // Items 1..N: Backends
    for (auto& backend_sock : backends) {
        items.push_back({static_cast<void*>(backend_sock), 0, ZMQ_POLLIN, 0});
    }

    std::cout << "[Gateway] Loop started. Listening...\n";

    while (!stopRequested) {
        // Poll with 100ms timeout so we can check stopRequested flag
        zmq::poll(items.data(), items.size(), std::chrono::milliseconds(100));

        // ---------------------------------------------------------
        // 1. CHECK FRONTEND (Client Requests)
        // ---------------------------------------------------------
        if (items[0].revents & ZMQ_POLLIN) {
            std::vector<zmq::message_t> request_parts;
            if (receive_multipart(frontend, request_parts)) {
                // Pick a broker based on the message content
                size_t broker_idx = choose_backend_index(request_parts);

                // Forward the ENTIRE message (Identity included) to the chosen
                // broker The Broker will see: [ClientID] [Empty] [Data]
                send_multipart(backends[broker_idx], request_parts);
            }
        }

        // ---------------------------------------------------------
        // 2. CHECK BACKENDS (Broker Replies)
        // ---------------------------------------------------------
        // We iterate from index 1 because index 0 is the frontend
        for (size_t i = 0; i < backends.size(); ++i) {
            if (items[i + 1].revents & ZMQ_POLLIN) {
                std::vector<zmq::message_t> reply_parts;

                // Read from the specific backend
                if (receive_multipart(backends[i], reply_parts)) {
                    // Route back to client.
                    // The reply from Broker will be: [ClientID] [Empty]
                    // [ReplyData] The Frontend (ROUTER) looks at the first
                    // frame (ClientID) and knows exactly which TCP connection
                    // to write to.
                    send_multipart(frontend, reply_parts);
                }
            }
        }
    }
    std::cout << "[Gateway] Loop stopped.\n";
}

// ---------------------------------------------------------
// Helpers
// ---------------------------------------------------------

bool Gateway::receive_multipart(zmq::socket_t& sock,
                                std::vector<zmq::message_t>& out_parts) {
    out_parts.clear();
    while (true) {
        zmq::message_t msg;
        auto res = sock.recv(msg, zmq::recv_flags::none);
        if (!res) return false;

        out_parts.push_back(std::move(msg));

        if (!sock.get(zmq::sockopt::rcvmore)) {
            break;
        }
    }
    return true;
}

bool Gateway::send_multipart(zmq::socket_t& sock,
                             std::vector<zmq::message_t>& parts) {
    for (size_t i = 0; i < parts.size(); ++i) {
        // ZMQ_SNDMORE for all frames except the last one
        auto flags = (i < parts.size() - 1) ? zmq::send_flags::sndmore
                                            : zmq::send_flags::none;
        sock.send(parts[i], flags);
    }
    return true;
}