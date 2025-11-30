# instructions to run
1. launch the brokers, note down their ip and port
2. write that in gateway.conf
3. launch the gateway, note down its ip and port
4. write that in client.conf before launching the client
5. launch the clients

## default run
1. open a terminal and run `./build/disk_node/disk_node `
2. open another terminal and run `./build/broker/broker 7001`
3. open another terminal and run `./build/broker/broker 7002`
4. open another terminal and run `./build/gateway/gateway`
5. open one or more terminals for clients with `./build/client/client`

this default config can run multiple clients, one gateway, two brokers, one disk_node.

# config files

## gateway.conf
first addr is gateway ip and port (used for binding)
second addr is heartbeat port of the gateway
second onwards are the ip and port of the brokers

## client.conf
first addr is ip and port addr of the broker

## broker.conf
first addr is the ip and port of the heartbeatEndpoint of the gateway
second addr is for the DB process

## disk_node.conf
listen addr of the disk