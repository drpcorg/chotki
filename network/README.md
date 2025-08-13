It implements a callback-driven architecture optimized for continuous bidirectional
streaming with automatic connection management and intelligent buffering.

# Architecture

Net is a basic networking for chotki. When creating a new net instance, we pass a callback (InstallCallback) that
creates a protocol handler for each connection (typically Syncer instance from replication package).
The protocol handler is responsible for
sending and receiving data to the connection.
Each duplex connection is represented by a Peer instance.

# Connection Management

Outbound: Connect("tcp://host:port") spawns persistent KeepConnecting() goroutines with
exponential backoff retry (500ms → 60s max). Each successful connection creates a Peer
that runs concurrent read/write loops until failure or close.

Inbound: Listen("tcp://:port") accepts connections and immediately wraps them in Peers
with the same bidirectional processing model.

# Connection flow

1. Connect() calls ConnectPool() with a single address

- This creates a connection pool entry with the name "tcp://localhost:8080"
- The entry is initially set to nil to prevent duplicate connections

2. ConnectPool() spawns a goroutine running KeepConnecting()

- This goroutine runs continuously until the network is closed
- It implements the retry logic with exponential backoff

3. KeepConnecting() attempts to establish the connection:

- Calls createConn() to create a TCP/TLS connection
- If connection fails, it waits with exponential backoff (0.5s → 1s → 2s → ... → 60s max)
- If connection succeeds, it calls keepPeer() to manage the connection

4. keepPeer() creates a new Peer instance:

- Calls the install callback to get a protocol handler
- Creates a Peer with the connection and configuration
- Stores the Peer in the connections map
- Calls Peer.Keep() to start read/write loops

5. Peer.Keep() runs two goroutines:

- keepRead(): Continuously reads from the socket, buffers data, and calls protocol.Drain()
- keepWrite(): Continuously calls protocol.Feed() and writes to the socket

6. If the connection fails or is closed:

- The Peer is removed from the connections map
- The destroy callback is called
- KeepConnecting() continues and will retry the connection

# Buffer Strategy for peer reads

Peer actually accumulates data in the buffer until it reaches one of the following thresholds:

- bufferMinToProcess: Efficiency threshold for normal operation
- bufferMaxSize: Memory protection limit (triggers immediate processing)
- readAccumTimeLimit: Latency protection timeout (default 5s)

This maximizes throughput by reducing protocol handler invocations while maintaining
bounded latency and memory usage. Specifically, it can reduce amount of pebble.Batch Merge() operations,
which reduces amount of disk I/O and improves performance.

Usage Example:

    // Create a new network instance with your protocol handler
    net := NewNet(logger, installCallback, destroyCallback,
    	&NetTlsConfigOpt{Config: tlsConfig},
    	&NetWriteTimeoutOpt{Timeout: 30 * time.Second},
    )

    // Start listening for incoming connections
    err := net.Listen("tcp://:8080")

    // Connect to a remote peer
    err = net.Connect("tcp://localhost:8080")

    // Clean up when done
    defer net.Close()
