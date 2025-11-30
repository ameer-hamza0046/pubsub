#include <common.hpp>
#include <functional>  // for std::hash
#include <gateway.hpp>
#include <iostream>

// Config: How long before we consider a broker dead?
const auto HEARTBEAT_TIMEOUT = std::chrono::milliseconds(1200);

Gateway::Gateway(const std::string& listenAddr,
                 const std::vector<std::string>& brokers, int heartbeatPort)
    : ctx(1),
      frontend(ctx, ZMQ_ROUTER),
      heartbeat_puller(ctx, ZMQ_PULL),  // Initialize PULL
      listenAddr(listenAddr),
      brokerAddrs(brokers) {
    // 1. Setup Frontend
    std::cout << "[Gateway] Binding frontend to " << listenAddr << "\n";
    frontend.bind(listenAddr);

    // 2. Setup Heartbeat Listener
    std::string hb_addr = "tcp://*:" + std::to_string(heartbeatPort);
    std::cout << "[Gateway] Listening for heartbeats on " << hb_addr << "\n";
    heartbeat_puller.bind(hb_addr);

    // 3. Setup Backends
    backends.reserve(brokers.size());
    node_states.reserve(brokers.size());

    for (size_t i = 0; i < brokers.size(); ++i) {
        const auto& addr = brokers[i];
        std::cout << "[Gateway] Connecting backend to " << addr << "\n";

        // Socket
        backends.emplace_back(ctx, ZMQ_DEALER);
        backends.back().connect(addr);

        // State (Assume Active initially)
        node_states.push_back({addr, true, std::chrono::steady_clock::now()});
        address_to_index[addr] = i;
    }

    // 4. Start Heartbeat Thread
    heartbeat_thread = std::thread(&Gateway::heartbeat_listener, this);
}

Gateway::~Gateway() { close(); }

void Gateway::close() {
    stopRequested = true;
    if (heartbeat_thread.joinable()) heartbeat_thread.join();
}

// Background Thread: Receives "I am alive" msgs and updates timers
void Gateway::heartbeat_listener() {
    while (!stopRequested) {
        // 1. Poll for heartbeats (non-blocking check)
        zmq::pollitem_t items[] = {
            {static_cast<void*>(heartbeat_puller), 0, ZMQ_POLLIN, 0}};
        zmq::poll(items, 1, std::chrono::milliseconds(200));

        if (items[0].revents & ZMQ_POLLIN) {
            zmq::message_t msg;
            if (heartbeat_puller.recv(msg, zmq::recv_flags::none)) {
                std::string addr(static_cast<char*>(msg.data()), msg.size());

                std::lock_guard<std::mutex> lock(registry_mutex);
                if (address_to_index.count(addr)) {
                    int idx = address_to_index[addr];
                    node_states[idx].last_seen =
                        std::chrono::steady_clock::now();

                    if (!node_states[idx].active) {
                        std::cout << "[Gateway] Broker " << idx
                                  << " is BACK online.\n";
                        node_states[idx].active = true;
                    }
                } else {
                    std::cout << "[Gateway] Received heartbeat from unknown "
                                 "broker: "
                              << addr << "\n";
                }
            }
        }

        // 2. Sweep for dead brokers
        {
            std::lock_guard<std::mutex> lock(registry_mutex);
            auto now = std::chrono::steady_clock::now();

            for (size_t i = 0; i < node_states.size(); ++i) {
                if (node_states[i].active) {
                    auto duration =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            now - node_states[i].last_seen);

                    if (duration > HEARTBEAT_TIMEOUT) {
                        std::cout << "[Gateway] Broker " << i << " ("
                                  << node_states[i].address
                                  << ") timed out! Marking inactive.\n";
                        node_states[i].active = false;
                    }
                }
            }
        }
    }
}

int Gateway::choose_backend_index(const std::vector<zmq::message_t>& parts) {
    if (parts.size() < 3) return -1;

    // Extract Key (Topic)
    const auto& payload_frame = parts[2];
    std::string key(static_cast<const char*>(payload_frame.data()),
                    payload_frame.size());
    auto parsed = split(key, DELIM);
    if (parsed.size() > 1) key = parsed[1];  // Use Topic

    size_t hash = std::hash<std::string>{}(key);

    std::lock_guard<std::mutex> lock(registry_mutex);

    // ROUND ROBIN SKIP LOGIC:
    // Start at hash index. If dead, try next.
    // If we loop back to start, everyone is dead.
    size_t start_index = hash % backends.size();
    size_t current_index = start_index;

    do {
        if (node_states[current_index].active) {
            return current_index;
        }
        current_index = (current_index + 1) % backends.size();
    } while (current_index != start_index);

    return -1;  // All dead
}

void Gateway::run() {
    std::vector<zmq::pollitem_t> items;

    // Item 0: Frontend
    items.push_back({static_cast<void*>(frontend), 0, ZMQ_POLLIN, 0});

    // Items 1..N: Backends
    // We poll ALL backends, even dead ones, just in case they sent a reply
    // right before dying, or if they are just slow.
    for (auto& backend_sock : backends) {
        items.push_back({static_cast<void*>(backend_sock), 0, ZMQ_POLLIN, 0});
    }

    std::cout << "[Gateway] Loop started. Listening...\n";

    while (!stopRequested) {
        zmq::poll(items.data(), items.size(), std::chrono::milliseconds(100));

        // 1. CHECK FRONTEND
        if (items[0].revents & ZMQ_POLLIN) {
            std::vector<zmq::message_t> request_parts;
            if (receive_multipart(frontend, request_parts)) {
                int idx = choose_backend_index(request_parts);

                if (idx != -1) {
                    send_multipart(backends[idx], request_parts);
                } else {
                    // All brokers down
                    std::cerr << "[Gateway] Drop: No active brokers.\n";
                    // send err to client
                    std::vector<zmq::message_t> err_parts;
                    err_parts.push_back(
                        std::move(request_parts[0]));       // client addr
                    err_parts.push_back(zmq::message_t());  // empty frame
                    std::string err_msg =
                        RESP_ERR + DELIM + "No active brokers";
                    err_parts.push_back(
                        zmq::message_t(err_msg.data(), err_msg.size()));
                    send_multipart(frontend, err_parts);
                }
            }
        }

        // 2. CHECK BACKENDS
        for (size_t i = 0; i < backends.size(); ++i) {
            if (items[i + 1].revents & ZMQ_POLLIN) {
                std::vector<zmq::message_t> reply_parts;
                if (receive_multipart(backends[i], reply_parts)) {
                    send_multipart(frontend, reply_parts);
                }
            }
        }
    }
    std::cout << "[Gateway] Loop stopped.\n";
}

// Helpers unchanged...
bool Gateway::receive_multipart(zmq::socket_t& sock,
                                std::vector<zmq::message_t>& out_parts) {
    out_parts.clear();
    while (true) {
        zmq::message_t msg;
        auto res = sock.recv(msg, zmq::recv_flags::none);
        if (!res) return false;
        out_parts.push_back(std::move(msg));
        if (!sock.get(zmq::sockopt::rcvmore)) break;
    }
    return true;
}

bool Gateway::send_multipart(zmq::socket_t& sock,
                             std::vector<zmq::message_t>& parts) {
    for (size_t i = 0; i < parts.size(); ++i) {
        auto flags = (i < parts.size() - 1) ? zmq::send_flags::sndmore
                                            : zmq::send_flags::none;
        sock.send(parts[i], flags);
    }
    return true;
}