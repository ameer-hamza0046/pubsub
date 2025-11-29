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

void Client::close() {
    if (!closed.exchange(true)) {
        std::cout << "Closing client socket...\n";
        socket.close();
        ctx.close();
    }
}
