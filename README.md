# instructions to run
1. launch the brokers, note down their ip and port
2. write that in gateway.conf
3. launch the gateway, note down its ip and port
4. write that in client.conf before launching the client
5. launch the clients

# gateway.conf
first addr is gateway ip and port (used for binding)
second addr is heartbeat endpoint of the gateway
second onwards are the ip and port of the brokers

# client.conf
first addr is ip and port addr of the broker

# broker.conf
first addr is the ip and port of the heartbeatEndpoint of the gateway