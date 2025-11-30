#include <broker.hpp>
#include <chrono>
#include <iostream>

Broker::Broker(const std::string& bindAddr, const std::string& gatewayHbAddr)
    : ctx(1), socket(ctx, ZMQ_ROUTER), bindAddr(bindAddr) {
    std::cout << "[Broker] Binding data to " << bindAddr << "\n";
    socket.bind(bindAddr);

    // Start the heartbeat thread immediately
    // We pass 'bindAddr' so the Gateway knows exactly WHO is sending the beat
    heartbeat_thread =
        std::thread(&Broker::heartbeat_routine, this, gatewayHbAddr, bindAddr);
}

Broker::~Broker() { close(); }

void Broker::close() {
    stopRequested = true;
    if (heartbeat_thread.joinable()) heartbeat_thread.join();
}

// --- NEW: Heartbeat Routine ---
void Broker::heartbeat_routine(std::string gatewayHbAddr, std::string myAddr) {
    // We use a separate context/socket for this thread for safety
    zmq::context_t hb_ctx(1);
    zmq::socket_t hb_sock(hb_ctx, ZMQ_PUSH);

    try {
        // Connect to the Gateway's PULL port
        hb_sock.connect(gatewayHbAddr);

        while (!stopRequested) {
            // Send our identity (myAddr) to say "I am alive"
            zmq::message_t msg(myAddr.data(), myAddr.size());

            // Send is usually non-blocking for PUSH unless pipe is full
            hb_sock.send(msg, zmq::send_flags::dontwait);

            // Sleep 1 second
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    } catch (const std::exception& e) {
        std::cerr << "[Broker HB] Error: " << e.what() << "\n";
    }
}

void Broker::run() {
    std::cout << "[Broker] Running. Waiting for requests...\n";

    while (!stopRequested) {
        // Poll with timeout to allow checking stopRequested
        zmq::pollitem_t items[] = {
            {static_cast<void*>(socket), 0, ZMQ_POLLIN, 0}};
        zmq::poll(items, 1, std::chrono::milliseconds(100));

        if (items[0].revents & ZMQ_POLLIN) {
            std::vector<zmq::message_t> parts;
            if (!receive_multipart(parts)) continue;

            // EXPECTED FRAME STACK:
            // [0] Gateway Connection ID (added by ZMQ on the broker side)
            // [1] Client ID (forwarded by Gateway)
            // [2] Empty Frame (delimiter from Client REQ)
            // [3] Payload (The actual command)

            if (parts.size() < 4) {
                std::cerr << "[Broker] Malformed message (too short)\n";
                continue;
            }

            // Extract the payload
            std::string payload(static_cast<char*>(parts[3].data()),
                                parts[3].size());
            std::cout << "[Broker] Received: " << payload << "\n";

            // Process Logic
            std::string replyStr = process_command(payload);

            // Construct Reply Stack
            // We must REUSE frames [0], [1], [2] to route back correctly
            // We replace [3] with our new response
            parts[3].rebuild(replyStr.data(), replyStr.size());

            // Send it back
            send_multipart(parts);
        }
    }
    std::cout << "[Broker] Stopped.\n";
}

std::string Broker::process_command(const std::string& payload) {
    auto tokens = split(payload, DELIM);
    if (tokens.empty()) return RESP_ERR + DELIM + "Empty";

    const std::string& cmd = tokens[0];

    // 1. PUBLISH
    if (cmd == CMD_PUBLISH && tokens.size() >= 3) {
        std::string topic = tokens[1];
        std::string msg = tokens[2];

        storage[topic].push_back(msg);

        std::cout << "   -> Stored msg on topic '" << topic << "'\n";
        return RESP_ACK;
    }

    // 2. GET LATEST ID
    else if (cmd == CMD_GET_LATEST_ID && tokens.size() >= 2) {
        std::string topic = tokens[1];
        if (storage.find(topic) == storage.end() || storage[topic].empty()) {
            return RESP_ERR;  // No messages
        }
        int id = storage[topic].size() - 1;
        return RESP_ID + DELIM + std::to_string(id);
    }

    // 3. GET MESSAGE BY ID
    else if (cmd == CMD_GET_MESSAGE_BY_ID && tokens.size() >= 3) {
        std::string topic = tokens[1];
        int id = -1;
        try {
            id = std::stoi(tokens[2]);
        } catch (...) {
        }

        if (storage.count(topic) && id >= 0 &&
            id < (int)storage[topic].size()) {
            return RESP_MSG + DELIM + storage[topic][id];
        } else {
            return RESP_ERR + DELIM + "NotFound";
        }
    }

    return RESP_ERR + DELIM + "UnknownCmd";
}

bool Broker::receive_multipart(std::vector<zmq::message_t>& out_parts) {
    out_parts.clear();
    while (true) {
        zmq::message_t msg;
        if (!socket.recv(msg, zmq::recv_flags::none)) return false;
        out_parts.push_back(std::move(msg));
        if (!socket.get(zmq::sockopt::rcvmore)) break;
    }
    return true;
}

bool Broker::send_multipart(std::vector<zmq::message_t>& parts) {
    for (size_t i = 0; i < parts.size(); ++i) {
        auto flags = (i < parts.size() - 1) ? zmq::send_flags::sndmore
                                            : zmq::send_flags::none;
        socket.send(parts[i], flags);
    }
    return true;
}