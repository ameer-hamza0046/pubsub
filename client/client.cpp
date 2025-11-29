#include "client.hpp"

#include <iostream>

Client::Client(const std::string& endpoint)
    : ctx(1), socket(ctx, ZMQ_REQ), endpoint(endpoint) {
    socket.connect(endpoint);
    std::cout << "Client connected to " << endpoint << std::endl;
}

Client::~Client() { close(); }

bool Client::send(const std::string& msg) {
    if (closed) return false;
    socket.send(zmq::buffer(msg), zmq::send_flags::none);
    return true;
}

bool Client::recv(std::string& replyOut) {
    if (closed) return false;

    zmq::message_t reply;
    auto ok = socket.recv(reply, zmq::recv_flags::none);
    if (!ok) return false;

    replyOut.assign(static_cast<char*>(reply.data()), reply.size());
    return true;
}

bool Client::publish(const std::string& topic, const std::string& msg) {
    if (closed) return false;

    std::string full_msg =
        CMD_PUBLISH + DELIM + topic + DELIM + msg;  // expected to receive ACK
    socket.send(zmq::buffer(full_msg), zmq::send_flags::none);

    // wait for reply
    zmq::message_t reply;
    auto ok = socket.recv(reply, zmq::recv_flags::none);
    if (!ok) return false;
    // parse
    auto replyStr = std::string(static_cast<char*>(reply.data()), reply.size());
    auto parsed = split(replyStr, DELIM);
    if (parsed.empty() or parsed[0] != RESP_ACK) return false;
    return true;
}

bool Client::get_latest_message_id(const std::string& topic,
                                   int& message_idOut) {
    if (closed) return false;

    std::string request =
        CMD_GET_LATEST_ID + DELIM + topic;  // expected to receive ID as string
    socket.send(zmq::buffer(request), zmq::send_flags::none);

    zmq::message_t reply;
    auto ok = socket.recv(reply, zmq::recv_flags::none);
    if (!ok) return false;

    std::string replyStr(static_cast<char*>(reply.data()), reply.size());
    message_idOut = std::stoi(replyStr);
    return true;
}

bool Client::get_message_by_id(const std::string& topic, int message_id,
                               std::string& messageOut) {
    if (closed) return false;

    std::string request =
        CMD_GET_MESSAGE_BY_ID + DELIM + topic + DELIM +
        std::to_string(message_id);  // expected to receive message
    socket.send(zmq::buffer(request), zmq::send_flags::none);

    zmq::message_t reply;
    auto ok = socket.recv(reply, zmq::recv_flags::none);
    if (!ok) return false;

    messageOut.assign(static_cast<char*>(reply.data()), reply.size());
    return true;
}

void Client::close() {
    if (!closed.exchange(true)) {
        std::cout << "Closing client socket...\n";
        socket.close();
        ctx.close();
    }
}
