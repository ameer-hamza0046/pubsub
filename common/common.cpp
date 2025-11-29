#pragma once

#include "common/common.hpp"

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

// Message Types
enum class MessageType {
    PUB,         // Publish a message
    READ,        // Read a specific message ID
    SUB_OFFSET,  // Get latest offset for a topic
    ACK,         // Acknowledge operation
    DATA,        // Return requested data
    HEARTBEAT    // I am alive
};

// The Data Unit
struct Packet {
    MessageType type;
    std::string topic;
    std::string payload;
    int msgId = -1;     // For Reads/Acks
    int brokerId = -1;  // For Heartbeats
};

// Helper for timestamps
inline long long getCurrentTimeMillis() {
    using namespace std::chrono;

    return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
        .count();
}