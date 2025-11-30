#include <algorithm>
#include <atomic>
#include <chrono>
#include <client.hpp>
#include <csignal>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

// --- GLOBALS & SYNCHRONIZATION ---

std::atomic<bool> g_running(true);
std::mutex g_cout_mutex;

// Stress Mode Globals
std::atomic<bool> g_stress_active(false);
std::thread g_stress_thread;

// Monitor Mode Globals
std::atomic<bool> g_monitor_active(false);
std::thread g_monitor_thread;
std::mutex g_sub_mutex;
// Map: TopicName -> LastKnownID
std::map<std::string, int> g_subscriptions;

// --- HELPER FUNCTIONS ---

void signal_handler(int) {
    g_running = false;
    g_stress_active = false;
    g_monitor_active = false;
    std::cout << "\n[MAIN] Interrupted. Shutting down threads...\n";
}

// Thread-safe print that preserves the user prompt
void safe_print(const std::string& msg) {
    std::lock_guard<std::mutex> lock(g_cout_mutex);
    std::cout << "\r" << msg << "\n> " << std::flush;
}

void print_help() {
    std::cout
        << "\n=== Distributed Client CLI (Multi-Topic) ===\n"
        << " --- General ---\n"
        << "  pub <topic> <msg>   : Publish a single message\n"
        << "  read <topic> <id>   : Read specific message\n"
        << "  latest <topic>      : Check latest ID manually\n"
        << "\n --- Monitoring (Multi-Topic) ---\n"
        << "  sub <topic>         : Add topic to subscription list\n"
        << "  unsub <topic>       : Remove topic from subscription list\n"
        << "  list                : Show active subscriptions\n"
        << "  monitor start       : Start background polling\n"
        << "  monitor stop        : Stop background polling\n"
        << "\n --- Stress Testing ---\n"
        << "  stress <topic> <N>  : Background spam (Publish N messages)\n"
        << "  stress stop         : Stop stress test\n"
        << "  exit                : Quit\n"
        << "============================================\n";
}

// --- BACKGROUND ROUTINES ---

void stress_routine(std::string endpoint, std::string topic, int count) {
    Client stressClient(endpoint);
    int sent = 0;
    while (g_stress_active && sent < count) {
        std::string msg = "stress-msg-" + std::to_string(sent);
        if (!stressClient.publish(topic, msg)) {
            // silent fail or retry
        }
        sent++;
        // Throttle slightly to not kill localhost loopback
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    std::stringstream ss;
    ss << "[Stress] Completed. Sent " << sent << " messages to '" << topic
       << "'.";
    safe_print(ss.str());
    g_stress_active = false;
}

void monitor_routine(std::string endpoint) {
    Client monitorClient(endpoint);
    safe_print("[Monitor] Background thread started.");

    while (g_monitor_active) {
        // 1. Snapshot the topics we need to check (minimize lock time)
        std::vector<std::string> topics_to_check;
        {
            std::lock_guard<std::mutex> lock(g_sub_mutex);
            for (const auto& pair : g_subscriptions) {
                topics_to_check.push_back(pair.first);
            }
        }

        if (topics_to_check.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        // 2. Iterate and Check Network
        for (const auto& topic : topics_to_check) {
            if (!g_monitor_active) break;

            int remote_id = -1;
            // Network Call (Slow, done without holding g_sub_mutex)
            if (monitorClient.get_latest_message_id(topic, remote_id)) {
                // Re-lock to check our local state (user might have unsubbed
                // meanwhile)
                std::lock_guard<std::mutex> lock(g_sub_mutex);

                // Check if topic still exists in map
                if (g_subscriptions.find(topic) != g_subscriptions.end()) {
                    int& local_id = g_subscriptions[topic];

                    // Fetch missing messages
                    if (remote_id > local_id) {
                        // Release lock to fetch messages (don't block UI)
                        // Note: We need a temporary copy of start/end ID
                        int start = local_id + 1;
                        int end = remote_id;

                        // We must unlock here to allow UI to work, but be
                        // careful
                        g_sub_mutex.unlock();

                        for (int id = start; id <= end; ++id) {
                            std::string msg;
                            if (monitorClient.get_message_by_id(topic, id,
                                                                msg)) {
                                std::stringstream ss;
                                ss << "[SUB: " << topic << "] #" << id << ": "
                                   << msg;
                                safe_print(ss.str());
                            }
                        }

                        // Re-lock to update final state
                        g_sub_mutex.lock();
                        // Verify topic exists again (sanity check)
                        if (g_subscriptions.count(topic)) {
                            g_subscriptions[topic] = end;
                        }
                    }
                }
            }
        }

        // 3. Polling Interval (1 second)
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    safe_print("[Monitor] Background thread stopped.");
}

// --- MAIN ---

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    const std::string config_path = "config/client.conf";
    std::string gatewayEndpoint;
    if (!read_one_line(config_path, gatewayEndpoint)) {
        gatewayEndpoint = "tcp://127.0.0.1:6000";
    }

    Client client(gatewayEndpoint);
    std::cout << "Connected to Gateway at " << gatewayEndpoint << "\n";
    print_help();

    std::string line;
    while (g_running) {
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line)) break;
        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string cmd;
        ss >> cmd;

        if (cmd == "exit")
            break;
        else if (cmd == "help")
            print_help();

        // --- PUBLISH ---
        else if (cmd == "pub") {
            std::string topic, msg_part;
            ss >> topic;
            std::getline(ss, msg_part);
            if (!msg_part.empty() && msg_part[0] == ' ') msg_part.erase(0, 1);

            if (topic.empty() || msg_part.empty())
                std::cout << "Usage: pub <topic> <msg>\n";
            else {
                if (client.publish(topic, msg_part))
                    safe_print("[OK] Published");
                else
                    safe_print("[ERR] Publish failed");
            }
        }

        // --- SUBSCRIPTION MANAGEMENT ---
        else if (cmd == "sub") {
            std::string topic;
            ss >> topic;
            if (topic.empty())
                std::cout << "Usage: sub <topic>\n";
            else {
                std::lock_guard<std::mutex> lock(g_sub_mutex);
                if (g_subscriptions.count(topic)) {
                    std::cout << "Already subscribed to " << topic << "\n";
                } else {
                    // Start ID at -1 (or query latest if you want to skip
                    // history) Let's query latest so we only see NEW messages
                    int latest = -1;
                    client.get_latest_message_id(topic, latest);
                    g_subscriptions[topic] = latest;
                    std::cout << "Subscribed to '" << topic
                              << "' starting at ID " << latest << "\n";
                }
            }
        } else if (cmd == "unsub") {
            std::string topic;
            ss >> topic;
            std::lock_guard<std::mutex> lock(g_sub_mutex);
            if (g_subscriptions.erase(topic))
                std::cout << "Unsubscribed from " << topic << "\n";
            else
                std::cout << "Topic not found in subscriptions.\n";
        } else if (cmd == "list") {
            std::lock_guard<std::mutex> lock(g_sub_mutex);
            std::cout << "--- Active Subscriptions ---\n";
            if (g_subscriptions.empty()) std::cout << "(None)\n";
            for (const auto& pair : g_subscriptions) {
                std::cout << " * " << pair.first << " (Last ID: " << pair.second
                          << ")\n";
            }
            std::cout << "----------------------------\n";
        }

        // --- MONITOR CONTROL ---
        else if (cmd == "monitor") {
            std::string subcmd;
            ss >> subcmd;
            if (subcmd == "start") {
                if (g_monitor_active)
                    std::cout << "Monitor already running.\n";
                else {
                    g_monitor_active = true;
                    if (g_monitor_thread.joinable()) g_monitor_thread.join();
                    g_monitor_thread =
                        std::thread(monitor_routine, gatewayEndpoint);
                }
            } else if (subcmd == "stop") {
                if (!g_monitor_active)
                    std::cout << "Monitor is not running.\n";
                else {
                    g_monitor_active =
                        false;  // Thread will check this and exit
                    std::cout << "Stopping monitor (please wait)...\n";
                }
            } else {
                std::cout << "Usage: monitor start | monitor stop\n";
            }
        }

        // --- STRESS MODE ---
        else if (cmd == "stress") {
            std::string subcmd;
            ss >> subcmd;
            if (subcmd == "stop") {
                g_stress_active = false;
            } else {
                // assume subcmd is topic
                std::string topic = subcmd;
                int count;
                ss >> count;
                if (topic.empty() || count <= 0) {
                    std::cout
                        << "Usage: stress <topic> <count> OR stress stop\n";
                } else {
                    if (g_stress_active)
                        std::cout << "Stress test already running.\n";
                    else {
                        std::cout << "Starting stress test on '" << topic
                                  << "'...\n";
                        g_stress_active = true;
                        if (g_stress_thread.joinable()) g_stress_thread.join();
                        g_stress_thread = std::thread(
                            stress_routine, gatewayEndpoint, topic, count);
                    }
                }
            }
        }

        // --- READ/LATEST ---
        else if (cmd == "latest") {
            std::string topic;
            ss >> topic;
            int id = -1;
            if (client.get_latest_message_id(topic, id))
                std::cout << "Latest ID: " << id << "\n";
            else
                std::cout << "Error getting ID.\n";
        } else if (cmd == "read") {
            std::string topic;
            int id;
            ss >> topic >> id;
            std::string msg;
            if (client.get_message_by_id(topic, id, msg))
                std::cout << "Msg: " << msg << "\n";
            else
                std::cout << "Msg not found.\n";
        } else {
            std::cout << "Unknown command.\n";
        }
    }

    // Cleanup
    g_monitor_active = false;
    g_stress_active = false;
    if (g_monitor_thread.joinable()) g_monitor_thread.join();
    if (g_stress_thread.joinable()) g_stress_thread.join();
    client.close();
    return 0;
}