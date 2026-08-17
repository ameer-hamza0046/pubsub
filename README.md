# Distributed Pub/Sub System

A high-performance, distributed Publish-Subscribe system built from scratch using **C++** and **ZeroMQ**. This project demonstrates advanced distributed systems concepts including load balancing, fault tolerance, caching, and disk persistence.

## Key Features

1. **Asynchronous Gateway:** Non-blocking routing that load-balances traffic using Consistent Hashing.
2. **Fault Tolerance:** Heartbeat mechanism detects dead brokers. The Gateway automatically reroutes traffic to active nodes.
3. **LRU Caching:** Brokers maintain in-memory LRU caches for hot data.
4. **Persistence:** A dedicated DB Node ensures data is written to disk (`storage.db`).
5. **Interactive Client:** Multi-threaded CLI client supporting Pub/Sub and stress testing.

## Build Instructions

### Prerequisites
* C++17 compliant compiler
* Meson and Ninja build system
* libzmq (ZeroMQ)

### Steps to Compile
1. Setup the build directory and compile the project:
```bash
make
```
2. You are good to go, make uses meson in the backend.

## Quick Start (Default Configurations)
To run the full system, open 5 separate terminals and run the commands in this exact order:

#### Terminal 1: The Database Node
```bash
./build/disk_node/disk_node
```
#### Terminal 2: The Gateway
```bash
./build/gateway/gateway
```

#### Terminal 3: Broker 1
```bash
./build/broker/broker 7001
```

#### Terminal 4: Broker 2
```bash
./build/broker/broker 7002
Terminal 5: The Client
```
#### Terminal 5: The Client
```bash
./build/client/client
```
You can create as many clients as you like.


## Configuration Files
The system relies on config files in the config/ directory.

1. gateway.conf
    Line 1: Gateway Bind Address (where clients connect).
    Line 2: Heartbeat Listener Port (where brokers send alive signals).
    Line 3+: Address of the Brokers.

2. broker.conf
    Line 1: Gateway Heartbeat Address (where to send signals).
    Line 2: DB Node Address (where to persist data).

3. client.conf
    Line 1: Gateway Address.

4. disk_node.conf
    Line 1: DB Node Bind Address.

## Client CLI Commands
Once the client is running, you can use these commands:

pub <topic> <msg> - Publish a message.
sub <topic> - Subscribe to a topic (monitor in background).
unsub <topic> - Stop monitoring.
latest <topic> - Get the ID of the newest message.
read <topic> <id> - Read a specific message.
stress <topic> <N> - Send N messages rapidly (stress test).
help - Show menu.
exit - Quit.

## Testing Fault Tolerance
To verify the system handles failures:

compiled

1. Start all nodes (DB, Gateway, Brokers, Client).
2. Run stress test 5000 in the Client.
3. Kill Broker 1 (Ctrl+C).
4. You will see the Gateway detect the timeout and route traffic to Broker 2.
5. Restart Broker 1.
6. You will see the Gateway detect it coming back online.
