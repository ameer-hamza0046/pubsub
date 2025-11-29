#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include <zmq.hpp>

#include "../../common/common.hpp"

class Gateway {
   public:
    Gateway(const std::string& bindAddress, int numWorkers = 4);
    ~Gateway();

    void start();  // start proxy (non-blocking)
    void stop();   // graceful shutdown

   private:
    void proxyLoop();            // runs zmq::proxy_steerable
    void workerRoutine(int id);  // per-worker logic

    std::atomic<bool> running{false};

    zmq::context_t ctx;
    zmq::socket_t frontend;  // ROUTER to clients
    zmq::socket_t backend;   // DEALER to workers
    zmq::socket_t control;   // REP control socket for proxy steering

    std::thread proxyThread;
    std::vector<std::thread> workers;

    std::string bindAddress;
};
