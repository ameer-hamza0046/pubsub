#pragma once

#include <fstream>
#include <map>
#include <string>
#include <vector>

class SimpleDB {
   public:
    SimpleDB(const std::string& filename);
    ~SimpleDB();

    // Returns the NEW ID assigned
    int insert(const std::string& topic, const std::string& msg);

    // Returns latest ID or -1 if empty
    int get_latest_id(const std::string& topic);

    // Linear scan to find message (simulating disk seek)
    // Returns empty string if not found
    std::string get_message(const std::string& topic, int id);

   private:
    std::string filename;
    std::ofstream db_writer;

    // Index: Topic -> Max ID (RAM)
    // We only keep the COUNT in RAM, not the messages.
    std::map<std::string, int> topic_counters;
};