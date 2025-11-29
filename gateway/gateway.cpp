#include "gateway.hpp"

#include <chrono>
#include <iostream>

Gateway::Gateway(const std::string& bindAddress, int numWorkers)
    : ctx(1),
      frontend(ctx, ZMQ_ROUTER),
      backend(ctx, ZMQ_DEALER),
      control(ctx, ZMQ_REP),
      bindAddress(bindAddress) {
    // Avoid hangs on close
    frontend.set(zmq::sockopt::linger, 0);
    backend.set(zmq::sockopt::linger, 0);
    control.set(zmq::sockopt::linger, 0);

    // Bind internal and external endpoints
    backend.bind("inproc://workers");
    control.bind("inproc://proxy-control");
    frontend.bind(bindAddress);

    // Spawn worker threads
    for (int i = 0; i < numWorkers; ++i) {
        workers.emplace_back(&Gateway::workerRoutine, this, i);
    }

    std::cout << "Gateway initialized at " << bindAddress << " with "
              << numWorkers << " workers.\n";
}

Gateway::~Gateway() { stop(); }

void Gateway::start() {
    if (running.exchange(true)) {
        return;  // already running
    }

    std::cout << "Starting proxy thread...\n";
    proxyThread = std::thread(&Gateway::proxyLoop, this);
}

void Gateway::proxyLoop() {
    std::cout << "[PROXY] Entering zmq::proxy_steerable...\n";
    try {
        zmq::proxy_steerable(frontend, backend,
                             zmq::socket_ref(),  // no capture socket
                             control             // REP control socket
        );
    } catch (const zmq::error_t& e) {
        // On shutdown, closing sockets can cause an error; we only log if not
        // shutting down
        if (running) {
            std::cout << "[PROXY] unexpected error: " << e.what() << "\n";
        }
    }
    std::cout << "[PROXY] zmq::proxy_steerable returned.\n";
}

void Gateway::stop() {
    if (!running.exchange(false)) {
        return;  // already stopped
    }

    std::cout << "Stopping gateway...\n";

    // 1) Send TERMINATE to proxy and wait for reply (REQ–REP handshake)
    try {
        zmq::socket_t terminator(ctx, ZMQ_REQ);
        terminator.set(zmq::sockopt::linger, 0);
        terminator.connect("inproc://proxy-control");

        std::cout << "[Shutdown] Sending TERMINATE...\n";
        terminator.send(zmq::buffer("TERMINATE"), zmq::send_flags::none);

        zmq::message_t reply;
        auto res = terminator.recv(reply, zmq::recv_flags::none);
        if (res) {
            std::cout << "[Shutdown] Proxy acknowledged TERMINATE.\n";
        } else {
            std::cout << "[Shutdown] No reply from proxy.\n";
        }

        // For older libzmq versions, TERMINATE alone may not break proxy;
        // closing frontend forces proxy_steerable to exit.
        std::cout << "[Shutdown] Closing frontend to force proxy exit (if "
                     "needed)...\n";
        frontend.close();
    } catch (const zmq::error_t& e) {
        std::cout << "[Shutdown] Error during TERMINATE: " << e.what() << "\n";
    }

    // 2) Join proxy thread
    std::cout << "[Shutdown] Joining proxy thread...\n";
    if (proxyThread.joinable()) {
        proxyThread.join();
    }

    // 3) Workers: we do NOT rely on backend.close() to wake them.
    // They use RCVTIMEO and running=false to exit.
    std::cout << "[Shutdown] Joining worker threads...\n";
    for (auto& t : workers) {
        if (t.joinable()) {
            t.join();
        }
    }

    // 4) Close remaining sockets and context
    std::cout << "[Shutdown] Closing backend, control, and context...\n";
    backend.close();
    control.close();
    ctx.close();

    std::cout << "Gateway shutdown complete.\n";
}

std::string construct_reply(const std::string& msg) {
    auto parsed = split(msg, DELIM);
    if (parsed.empty()) {
        return RESP_ERR + DELIM + "Empty message";
    }
    if (parsed[0] == CMD_PUBLISH) {
        // Simulate processing publish
        std::cout << "[Worker] Publishing to topic: " << parsed[1]
                  << " Message: " << parsed[2] << "\n";
        return RESP_ACK + DELIM + "Published";
    } else if (parsed[0] == CMD_GET_LATEST_ID) {
        // Simulate fetching latest message ID
        int fake_id = 42;  // placeholder
        return RESP_ID + DELIM + std::to_string(fake_id);
    } else if (parsed[0] == CMD_GET_MESSAGE_BY_ID) {
        // Simulate fetching message by ID
        std::string fake_message = "This is a message with ID " + parsed[1];
        return RESP_MSG + DELIM + fake_message;
    } else {
        return RESP_ERR + DELIM + "Unknown command";
    }
}

void Gateway::workerRoutine(int id) {
    try {
        zmq::socket_t worker(ctx, ZMQ_REP);
        worker.set(zmq::sockopt::linger, 0);

        // KEY: Give recv a timeout so we can check `running` periodically
        worker.set(zmq::sockopt::rcvtimeo, 200);  // 200 ms

        worker.connect("inproc://workers");

        while (true) {
            zmq::message_t req;
            auto res = worker.recv(req, zmq::recv_flags::none);

            if (!res) {
                // Timeout or interruption.
                if (!running) {
                    std::cout << "[Worker " << id
                              << "] Shutdown detected, exiting.\n";
                    break;
                }
                // Otherwise, just continue waiting.
                continue;
            }

            std::string msg(static_cast<char*>(req.data()), req.size());
            std::cout << "[Worker " << id << "] received: " << msg << "\n";

            // frame the reply
            std::string reply = construct_reply(msg);

            worker.send(zmq::buffer(reply), zmq::send_flags::none);
        }
    } catch (const zmq::error_t& e) {
        if (e.num() != ETERM) {
            std::cerr << "[Worker " << id << "] ZMQ error: " << e.what()
                      << "\n";
        } else {
            std::cout << "[Worker " << id << "] Context terminating.\n";
        }
    }
}
