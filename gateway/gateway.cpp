#include "gateway.hpp"

#include <chrono>
#include <iostream>
#include <string>

Gateway::Gateway(const std::string& bindAddress, int numWorkers)
    : ctx(1), frontend(ctx, ZMQ_ROUTER), backend(ctx, ZMQ_DEALER) {
    // Bind ROUTER externally (client connections)
    frontend.bind(bindAddress);

    // Bind DEALER internally for workers
    backend.bind("inproc://workers");

    // Spawn worker threads
    for (int i = 0; i < numWorkers; ++i) {
        workers.emplace_back(&Gateway::workerRoutine, this, i);
    }

    std::cout << "Gateway online at " << bindAddress
              << ", workers = " << numWorkers << "\n";
}

Gateway::~Gateway() { stop(); }

void Gateway::start() {
    // ROUTER <-> DEALER message routing (blocks forever)
    zmq::proxy(frontend, backend);
}

void Gateway::stop() {
    if (!running) return;
    running = false;

    std::cout << "Shutting down Gateway ...\n";

    // terminate workers
    for (auto& t : workers) {
        if (t.joinable()) t.detach();  // proxy never returns normally
    }

    frontend.close();
    backend.close();
    ctx.close();
}

void Gateway::workerRoutine(int id) {
    zmq::socket_t worker(ctx, ZMQ_REP);
    worker.connect("inproc://workers");

    while (running) {
        zmq::message_t req;
        auto res = worker.recv(req, zmq::recv_flags::none);
        if (!res) continue;  // interrupted / shutting down

        std::string msg(static_cast<char*>(req.data()), req.size());
        std::cout << "[Worker " << id << "] received: " << msg << std::endl;

        // ───────── Business Logic ─────────────────────
        // For now: if msg == "hello", reply "serverHello"
        std::string reply = "serverHello";
        // ──────────────────────────────────────────────

        worker.send(zmq::buffer(reply), zmq::send_flags::none);
    }
}
