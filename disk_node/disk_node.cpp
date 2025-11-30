#include <atomic>
#include <common.hpp>
#include <csignal>
#include <disk_node.hpp>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include <zmq.hpp>

SimpleDB::SimpleDB(const std::string& filename) : filename(filename) {
    // 1. Load/Index the file to find the latest IDs
    std::ifstream file(filename);
    if (file.is_open()) {
        std::string line;
        std::cout << "[DB] Indexing existing data...\n";
        while (std::getline(file, line)) {
            // Format: TOPIC;ID;MSG
            auto parts = split(line, DELIM);
            if (parts.size() >= 3) {
                std::string topic = parts[0];
                try {
                    int id = std::stoi(parts[1]);
                    // Keep track of the highest ID seen for this topic
                    if (id > topic_counters[topic]) {
                        topic_counters[topic] = id;
                    }
                } catch (...) {
                }
            }
        }
    }
    // Open for appending
    db_writer.open(filename, std::ios::app);
}

SimpleDB::~SimpleDB() {
    if (db_writer.is_open()) db_writer.close();
}

// Returns the NEW ID assigned
int SimpleDB::insert(const std::string& topic, const std::string& msg) {
    int next_id = 0;
    if (topic_counters.count(topic)) {
        next_id = topic_counters[topic] + 1;
    }

    // Update Index
    topic_counters[topic] = next_id;

    // Write to Disk
    // Format: TOPIC;ID;MSG
    if (db_writer.is_open()) {
        db_writer << topic << DELIM << next_id << DELIM << msg << "\n";
        db_writer.flush();
    }
    return next_id;
}

int SimpleDB::get_latest_id(const std::string& topic) {
    if (topic_counters.count(topic)) {
        return topic_counters[topic];
    }
    return -1;  // Topic not found
}

// Linear scan to find message (Simulating Disk Seek)
std::string SimpleDB::get_message(const std::string& topic, int id) {
    std::ifstream reader(filename);
    if (!reader.is_open()) return "";

    std::string line;
    // Optimization: In a real DB, we would use offsets.
    // Here we scan. It's safe because we only scan on Cache Miss.
    while (std::getline(reader, line)) {
        auto parts = split(line, DELIM);
        if (parts.size() >= 3) {
            if (parts[0] == topic) {
                try {
                    if (std::stoi(parts[1]) == id) {
                        return parts[2];  // Found it
                    }
                } catch (...) {
                }
            }
        }
    }
    return "";  // Not found
}