#pragma once
#include <fstream>
#include <string>
#include <vector>
#include <iostream>

// Prefixes
inline const std::string CMD_PUBLISH = "PUBLISH";
inline const std::string CMD_GET_LATEST_ID = "GET_LATEST_ID";
inline const std::string CMD_GET_MESSAGE_BY_ID = "GET_MESSAGE_BY_ID";

// Responses
inline const std::string RESP_ACK = "ACK";  // for publish confirmations
inline const std::string RESP_ID = "ID";    // for latest message ID responses
inline const std::string RESP_MSG = "MSG";  // for message retrieval responses
inline const std::string RESP_ERR = "ERR";

// Delimiter
inline const char DELIM = ';';

// Helper functions
inline std::vector<std::string> split(const std::string& s,
                                      char delim = DELIM) {
    std::vector<std::string> v;
    std::string cur;
    for (char c : s) {
        if (c == delim) {
            v.push_back(cur);
            cur.clear();
        } else
            cur += c;
    }
    v.push_back(cur);
    return v;
}

// for reading config files
inline bool read_one_line(const std::string& filename,
                          std::string& endpointOut) {
    std::ifstream fin(filename);
    if (!fin.is_open()) return false;

    std::string line;
    if (!std::getline(fin, line)) return false;

    // trim whitespace
    line.erase(0, line.find_first_not_of(" \t\r\n"));
    line.erase(line.find_last_not_of(" \t\r\n") + 1);

    if (line.empty()) return false;

    endpointOut = line;
    return true;
}

inline bool read_all_lines(const std::string& filename,
                           std::vector<std::string>& out) {
    std::ifstream fin(filename);
    if (!fin.is_open()) return false;
    std::string line;
    while (std::getline(fin, line)) {
        line.erase(0, line.find_first_not_of(" \t\r\n"));
        line.erase(line.find_last_not_of(" \t\r\n") + 1);
        if (!line.empty()) out.push_back(line);
    }
    return !out.empty();
}
