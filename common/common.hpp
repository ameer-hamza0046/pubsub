#pragma once
#include <string>
#include <vector>

// Prefixes / verbs
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

// Helper functions (optional)
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
