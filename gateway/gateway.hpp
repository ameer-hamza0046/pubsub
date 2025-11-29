#pragma once
#include <thread>
#include <vector>
#include <zmq.hpp>

class Gateway {
   public:
    Gateway(const std::string& bindAddress, int numWorkers = 4);
    ~Gateway();

    void start();  // blocking
    void stop();   // graceful shutdown

   private:
    void workerRoutine(int id);

    zmq::context_t ctx;
    zmq::socket_t frontend;  // ROUTER
    zmq::socket_t backend;   // DEALER
    std::vector<std::thread> workers;
    bool running = true;
};
