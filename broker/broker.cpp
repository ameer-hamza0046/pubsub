#include <broker.hpp>

#include <chrono>
#include <iostream>

// Cache Size Limit
const size_t CACHE_SIZE = 512;

Broker::Broker(const std::string& bindAddr, const std::string& gatewayHbAddr,
               const std::string& dbNodeAddr)
    : ctx(1),
      socket(ctx, ZMQ_ROUTER),
      db_socket(ctx, ZMQ_REQ),  // REQ because DB is REP
      bindAddr(bindAddr),
      latest_id_cache(CACHE_SIZE),
      msg_cache(CACHE_SIZE) {
    std::cout << "[Broker] Binding data to " << bindAddr << "\n";
    socket.bind(bindAddr);

    std::cout << "[Broker] Connecting to DB Node at " << dbNodeAddr << "\n";
    db_socket.connect(dbNodeAddr);

    // Start Heartbeat
    heartbeat_thread =
        std::thread(&Broker::heartbeat_routine, this, gatewayHbAddr, bindAddr);
}

Broker::~Broker() { close(); }

void Broker::close() {
    stopRequested = true;
    if (heartbeat_thread.joinable()) heartbeat_thread.join();
}

void Broker::heartbeat_routine(std::string gatewayHbAddr, std::string myAddr) {
    zmq::context_t hb_ctx(1);
    zmq::socket_t hb_sock(hb_ctx, ZMQ_PUSH);
    hb_sock.set(zmq::sockopt::linger, 0);

    try {
        hb_sock.connect(gatewayHbAddr);
        while (!stopRequested) {
            zmq::message_t msg(myAddr.data(), myAddr.size());
            try {
                hb_sock.send(msg, zmq::send_flags::dontwait);
            } catch (const zmq::error_t&) { /* Ignore EAGAIN */
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    } catch (...) {
    }
}

void Broker::run() {
    std::cout << "[Broker] Running...\n";
    while (!stopRequested) {
        zmq::pollitem_t items[] = {
            {static_cast<void*>(socket), 0, ZMQ_POLLIN, 0}};
        zmq::poll(items, 1, std::chrono::milliseconds(100));

        if (items[0].revents & ZMQ_POLLIN) {
            std::vector<zmq::message_t> parts;
            if (!receive_multipart(parts)) continue;

            if (parts.size() < 4) {
                std::cerr << "[Broker] Malformed msg\n";
                continue;
            }

            std::string payload(static_cast<char*>(parts[3].data()),
                                parts[3].size());
            // Process
            std::string replyStr = process_command(payload);

            // Reply
            parts[3].rebuild(replyStr.data(), replyStr.size());
            send_multipart(parts);
        }
    }
}

// Helper to talk to DB Node (Blocking for simplicity)
std::string Broker::query_db(const std::string& requestStr) {
    try {
        std::cout << "[Broker] Querying DB: " << requestStr << "\n";
        zmq::message_t req(requestStr.data(), requestStr.size());
        db_socket.send(req, zmq::send_flags::none);

        zmq::message_t rep;
        auto res = db_socket.recv(rep, zmq::recv_flags::none);
        if (!res) return RESP_ERR;

        return std::string(static_cast<char*>(rep.data()), rep.size());
    } catch (const std::exception& e) {
        std::cerr << "[Broker] DB Query Error: " << e.what() << "\n";
        return RESP_ERR;
    }
}

std::string Broker::process_command(const std::string& payload) {
    auto tokens = split(payload, DELIM);
    if (tokens.empty()) return RESP_ERR + DELIM + "Empty";

    const std::string& cmd = tokens[0];

    // --------------------------------------------------------
    // 1. PUBLISH -> Write Through (DB + Cache)
    // --------------------------------------------------------
    if (cmd == CMD_PUBLISH && tokens.size() >= 3) {
        std::string topic = tokens[1];
        std::string msg = tokens[2];

        // A. Write to DB first (Source of Truth) to get the ID
        // DB Protocol: PUBLISH;TOPIC;MSG
        std::string dbResp = query_db(payload);

        auto dbParts = split(dbResp, DELIM);
        if (dbParts.empty() || dbParts[0] != RESP_ACK) {
            return RESP_ERR + DELIM + "DB_Write_Failed";
        }

        // DB returns ACK;ID
        std::string new_id_str = (dbParts.size() >= 2) ? dbParts[1] : "0";

        // B. Update Caches (Write Allocation)

        // Cache 1: Latest ID
        latest_id_cache.put(topic, new_id_str);

        // Cache 2: The Message itself
        // Key: "TOPIC;ID"
        std::string msgKey = topic + DELIM + new_id_str;
        msg_cache.put(msgKey, msg);

        std::cout << "   -> Pub Cached & Persisted. ID: " << new_id_str << "\n";
        return RESP_ACK;
    }

    // --------------------------------------------------------
    // 2. GET LATEST ID -> Read Aside (Cache -> DB)
    // --------------------------------------------------------
    else if (cmd == CMD_GET_LATEST_ID && tokens.size() >= 2) {
        std::string topic = tokens[1];
        std::string val;

        // A. Check Cache
        if (latest_id_cache.get(topic, val)) {
            return RESP_ID + DELIM + val;  // Hit
        }

        // B. Cache Miss -> Ask DB
        // DB Protocol: GET_LATEST_ID;TOPIC
        std::string dbResp =
            query_db(payload);  // Returns just the ID string or -1

        // C. Update Cache and Return
        if (dbResp != "-1" && !dbResp.empty()) {
            latest_id_cache.put(topic, dbResp);
            return RESP_ID + DELIM + dbResp;
        } else {
            return RESP_ID + DELIM + "-1";
        }
    }

    // --------------------------------------------------------
    // 3. GET MSG BY ID -> Read Aside (Cache -> DB)
    // --------------------------------------------------------
    else if (cmd == CMD_GET_MESSAGE_BY_ID && tokens.size() >= 3) {
        std::string topic = tokens[1];
        std::string idStr = tokens[2];
        std::string key = topic + DELIM + idStr;
        std::string msgVal;

        // A. Check Cache
        if (msg_cache.get(key, msgVal)) {
            return RESP_MSG + DELIM + msgVal;  // Hit
        }

        // B. Cache Miss -> Ask DB
        // DB Protocol: GET_MESSAGE_BY_ID;TOPIC;ID
        std::string dbResp = query_db(payload);

        // C. Update Cache and Return
        if (dbResp != RESP_ERR && !dbResp.empty()) {
            msg_cache.put(key, dbResp);
            return RESP_MSG + DELIM + dbResp;
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