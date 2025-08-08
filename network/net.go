// Provides a high-performance TCP/TLS server and client implementation
// for real-time asynchronous communication. This package is designed for continuous
// bidirectional message streaming with high throughput and low latency, unlike
// traditional request-response patterns (like HTTP).
//
// ARCHITECTURE OVERVIEW
// ====================
//
// The network package uses a callback-based architecture where you provide a Protocol
// Handler that already knows how to process data. The Net layer provides the transport
// mechanism, while your Protocol Handler handles the actual data processing and
// business logic.
//
// Key Components:
//   - Net: Manages connections, listeners, and network transport
//   - Peer: Handles individual connection lifecycle and data buffering
//   - Protocol Handler: Your application logic for data processing (Feed/Drain)
//
// Key Features:
//   - Support for TCP and TLS protocols
//   - Automatic connection management with exponential backoff retry logic
//   - Configurable buffer sizes and processing thresholds
//   - Thread-safe concurrent operations with goroutines
//   - Graceful connection handling and resource cleanup
//   - Bidirectional data streaming with buffering and batching
//
// CONNECTION ESTABLISHMENT FLOW
// =============================
//
// The network package uses a callback-based architecture where you provide a Protocol Handler
// that already knows how to process data. When you create a Net instance with NewNet(),
// you pass an install callback that returns a FeedDrainCloserTraced interface. This
// Protocol Handler is the core component that:
//   - Implements Feed() for outgoing data (application → network)
//   - Implements Drain() for incoming data (network → application)
//   - Handles protocol parsing and message processing
//   - Contains the business logic for data handling
//
// When connections are established, the Net layer calls your install callback to get
// a Protocol Handler instance for each connection, and the Peer uses this handler
// for all data processing through the Feed()/Drain() methods.
//
// When you call Connect("tcp://localhost:8080"), here's what happens:
//
// 1. Connect() calls ConnectPool() with a single address
//   - This creates a connection pool entry with the name "tcp://localhost:8080"
//   - The entry is initially set to nil to prevent duplicate connections
//
// 2. ConnectPool() spawns a goroutine running KeepConnecting()
//   - This goroutine runs continuously until the network is closed
//   - It implements the retry logic with exponential backoff
//
// 3. KeepConnecting() attempts to establish the connection:
//   - Calls createConn() to create a TCP/TLS connection
//   - If connection fails, it waits with exponential backoff (0.5s → 1s → 2s → ... → 60s max)
//   - If connection succeeds, it calls keepPeer() to manage the connection
//
// 4. keepPeer() creates a new Peer instance:
//   - Calls the install callback to get a protocol handler
//   - Creates a Peer with the connection and configuration
//   - Stores the Peer in the connections map
//   - Calls Peer.Keep() to start read/write loops
//
// 5. Peer.Keep() runs two goroutines:
//   - keepRead(): Continuously reads from the socket, buffers data, and calls protocol.Drain()
//   - keepWrite(): Continuously calls protocol.Feed() and writes to the socket
//
// 6. If the connection fails or is closed:
//   - The Peer is removed from the connections map
//   - The destroy callback is called
//   - KeepConnecting() continues and will retry the connection
//
// READ BUFFERING STRATEGY
// =======================
//
// The read buffering system accumulates data from the network socket until
// specific thresholds are met. This batching is crucial because the larger
// the batch, the fewer resources are consumed by the Protocol Handler
// (the entity passed during Net creation) for saving and processing data.
//
// Buffering Thresholds:
//   - bufferMinToProcess: Minimum data to accumulate before processing
//   - bufferMaxSize: Maximum buffer size to prevent memory exhaustion
//   - readAccumTimeLimit: Maximum time to wait for more data (default: 5s)
//
// Processing Triggers:
// Buffer is processed when ANY condition is met:
//  1. Buffer size reaches bufferMinToProcess (efficiency trigger)
//  2. Buffer size reaches bufferMaxSize (memory protection trigger)
//  3. Time since last read exceeds readAccumTimeLimit (latency trigger)
//
// Key Benefits:
//   - Larger batches reduce CPU overhead for Protocol Handler operations
//   - Fewer calls to Protocol.Drain() means better performance
//   - Balances latency (small batches) vs efficiency (large batches)
//   - Concurrent processing prevents blocking network reads
//
// Usage Example:
//
//	// Create a new network instance with your protocol handler
//	net := NewNet(logger, installCallback, destroyCallback,
//		&NetTlsConfigOpt{Config: tlsConfig},
//		&NetWriteTimeoutOpt{Timeout: 30 * time.Second},
//	)
//
//	// Start listening for incoming connections
//	err := net.Listen("tcp://:8080")
//
//	// Connect to a remote peer
//	err = net.Connect("tcp://localhost:8080")
//
//	// Clean up when done
//	defer net.Close()
package network

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/utils"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v3"
)

// ConnType represents the type of network connection
type ConnType = uint

var (
	// ErrAddressInvalid is returned when the provided address format is invalid
	ErrAddressInvalid = errors.New("the address invalid")
	// ErrAddressDuplicated is returned when attempting to use an address that's already in use
	ErrAddressDuplicated = errors.New("the address already used")
	// ErrAddressUnknown is returned when trying to disconnect from an unknown address
	ErrAddressUnknown = errors.New("address unknown")
	// ErrDisconnected is returned when a connection is closed by the user
	ErrDisconnected = errors.New("disconnected by user")
)

const (
	TCP ConnType = iota + 1
	TLS
	QUIC
)

const (
	// TYPICAL_MTU is the typical Maximum Transmission Unit size
	TYPICAL_MTU = 1500
	// MAX_OUT_QUEUE_LEN is the maximum length of the output queue (16MB of pointers)
	MAX_OUT_QUEUE_LEN = 1 << 20

	// MAX_RETRY_PERIOD is the maximum time to wait between connection retry attempts
	MAX_RETRY_PERIOD = time.Minute
	// MIN_RETRY_PERIOD is the minimum time to wait between connection retry attempts
	MIN_RETRY_PERIOD = time.Second / 2
)

type InstallCallback func(name string) protocol.FeedDrainCloserTraced
type DestroyCallback func(name string, p protocol.Traced)

// Net provides a TCP/TLS/QUIC server/client for real-time async communication.
// Unlike request-response patterns (like HTTP), this implementation constantly
// sends many tiny messages without waiting for responses. This requires different
// work patterns than typical HTTP/RPC servers, as one slow receiver cannot delay
// event transmission to all other receivers.
type Net struct {
	wg        sync.WaitGroup
	log       utils.Logger
	onInstall InstallCallback
	onDestroy DestroyCallback

	conns     *xsync.MapOf[string, *Peer]
	listens   *xsync.MapOf[string, net.Listener]
	ctx       context.Context
	cancelCtx context.CancelFunc

	tlsConfig          *tls.Config
	readBufferTcpSize  int
	writeBufferTcpSize int
	readAccumTimeLimit time.Duration
	writeTimeout       time.Duration
	bufferMaxSize      int
	bufferMinToProcess int
}

type NetOpt interface {
	Apply(*Net)
}

type NetWriteTimeoutOpt struct {
	Timeout time.Duration
}

func (opt *NetWriteTimeoutOpt) Apply(n *Net) {
	n.writeTimeout = opt.Timeout
}

type NetTlsConfigOpt struct {
	Config *tls.Config
}

func (opt *NetTlsConfigOpt) Apply(n *Net) {
	n.tlsConfig = opt.Config
}

type NetReadBatchOpt struct {
	ReadAccumTimeLimit time.Duration
	BufferMaxSize      int
	BufferMinToProcess int
}

func (opt *NetReadBatchOpt) Apply(n *Net) {
	n.readAccumTimeLimit = opt.ReadAccumTimeLimit
	n.bufferMaxSize = opt.BufferMaxSize
	n.bufferMinToProcess = opt.BufferMinToProcess
}

type TcpBufferSizeOpt struct {
	Read  int
	Write int
}

func (opt *TcpBufferSizeOpt) Apply(n *Net) {
	n.readBufferTcpSize = opt.Read
	n.writeBufferTcpSize = opt.Write
}

// NewNet creates a new network instance with the specified logger and callbacks.
// Additional configuration can be provided through NetOpt parameters.
//
// Example:
//
//	net := NewNet(logger, installCallback, destroyCallback,
//		&NetTlsConfigOpt{Config: tlsConfig},
//		&NetWriteTimeoutOpt{Timeout: 30 * time.Second},
//	)
func NewNet(log utils.Logger, install InstallCallback, destroy DestroyCallback, opts ...NetOpt) *Net {
	ctx, cancel := context.WithCancel(context.Background())
	net := &Net{
		log:       log,
		cancelCtx: cancel,
		ctx:       ctx,
		conns:     xsync.NewMapOf[string, *Peer](),
		listens:   xsync.NewMapOf[string, net.Listener](),
		onInstall: install,
		onDestroy: destroy,
	}
	for _, o := range opts {
		o.Apply(net)
	}
	return net
}

type NetStats struct {
	ReadBuffers  map[string]int32
	WriteBatches map[string]int32
}

func (n *Net) GetStats() NetStats {
	stats := NetStats{
		ReadBuffers:  make(map[string]int32),
		WriteBatches: make(map[string]int32),
	}
	n.conns.Range(func(name string, peer *Peer) bool {
		if peer != nil {
			stats.ReadBuffers[name] = peer.GetIncomingPacketBufferSize()
			stats.WriteBatches[name] = int32(peer.writeBatchSize.Val())
		}
		return true
	})
	return stats
}

func (n *Net) Close() error {
	n.cancelCtx()

	n.listens.Range(func(_ string, v net.Listener) bool {
		v.Close()
		return true
	})
	n.listens.Clear()

	n.conns.Range(func(_ string, p *Peer) bool {
		// sometimes it can be nil when we started connecting, but haven't connected yet
		if p != nil {
			p.Close()
		}
		return true
	})
	n.conns.Clear()

	n.wg.Wait()
	return nil
}

func (n *Net) Connect(addr string) (err error) {
	return n.ConnectPool(addr, []string{addr})
}

// ConnectPool establishes connections to multiple addresses with automatic failover.
// The connection will attempt to connect to each address in the provided list,
// and will retry with exponential backoff if all addresses fail.
//
// The name parameter is used to identify this connection pool in logs and callbacks.
func (n *Net) ConnectPool(name string, addrs []string) (err error) {
	// nil is needed so that Connect cannot be called
	// while KeepConnecting is connects
	if _, ok := n.conns.LoadOrStore(name, nil); ok {
		return ErrAddressDuplicated
	}

	n.wg.Add(1)
	go func() {
		n.KeepConnecting(fmt.Sprintf("connect:%s", name), addrs)
		n.wg.Done()
	}()

	return nil
}

func (de *Net) Disconnect(name string) (err error) {
	conn, ok := de.conns.LoadAndDelete(name)
	if !ok {
		return ErrAddressUnknown
	}

	conn.Close()
	return nil
}

// Listen starts listening for incoming connections on the specified address.
// The address should be in the format "tcp://:port", "tls://:port", or "quic://:port".
// Returns ErrAddressDuplicated if already listening on this address.
func (n *Net) Listen(addr string) error {
	// nil is needed so that Listen cannot be called
	// while creating listener
	if _, ok := n.listens.LoadOrStore(addr, nil); ok {
		return ErrAddressDuplicated
	}

	listener, err := n.createListener(addr)
	if err != nil {
		n.listens.Delete(addr)
		return err
	}
	n.listens.Store(addr, listener)

	n.log.Info("net: listening", "addr", addr)

	n.wg.Add(1)
	go func() {
		n.KeepListening(addr)
		n.wg.Done()
	}()

	return nil
}

func (de *Net) Unlisten(addr string) error {
	listener, ok := de.listens.LoadAndDelete(addr)
	if !ok {
		return ErrAddressUnknown
	}

	return listener.Close()
}

// KeepConnecting continuously attempts to maintain a connection to the provided addresses.
// It implements exponential backoff retry logic and will attempt to connect to each
// address in the list until a successful connection is established.
func (n *Net) KeepConnecting(name string, addrs []string) {
	connBackoff := MIN_RETRY_PERIOD
	for n.ctx.Err() == nil {
		var err error
		var conn net.Conn
		for _, addr := range addrs {
			conn, err = n.createConn(addr)
			if err == nil {
				break
			}
		}

		if err != nil {
			n.log.Error("net: couldn't connect", "name", name, "err", err)

			select {
			case <-time.After(connBackoff):
			case <-n.ctx.Done():
				break
			}
			connBackoff = min(MAX_RETRY_PERIOD, connBackoff*2)

			continue
		}
		n.setTCPBuffersSize(n.log.WithDefaultArgs(context.Background(), "name", name), conn)
		n.log.Info("net: connected", "name", name)

		connBackoff = MIN_RETRY_PERIOD
		n.keepPeer(name, conn)
	}
}

// setTCPBuffersSize configures TCP buffer sizes for the given connection.
// It handles both plain TCP connections and TLS-wrapped connections.
func (n *Net) setTCPBuffersSize(ctx context.Context, conn net.Conn) {
	var tconn *net.TCPConn
	switch res := conn.(type) {
	case *tls.Conn:
		nconn, ok := res.NetConn().(*net.TCPConn)
		if !ok {
			n.log.WarnCtx(ctx, "net: unable to set buffers, because tls conn is strange")
			return
		}
		tconn = nconn
	case *net.TCPConn:
		tconn = res
	default:
		n.log.WarnCtx(ctx, "net: unable to set buffers, because unknown connection type")
		return
	}
	if n.readBufferTcpSize > 0 {
		tconn.SetReadBuffer(n.readBufferTcpSize)
	}
	if n.writeBufferTcpSize > 0 {
		tconn.SetWriteBuffer(n.writeBufferTcpSize)
	}
}

// KeepListening continuously accepts incoming connections on the specified address.
// For each accepted connection, it spawns a goroutine to handle the peer communication.
func (n *Net) KeepListening(addr string) {
	for n.ctx.Err() == nil {
		listener, ok := n.listens.Load(addr)
		if !ok {
			break
		}

		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}

			// reconnects are the client's problem, just continue
			n.log.Error("net: couldn't accept request", "addr", addr, "err", err)
			continue
		}

		remoteAddr := conn.RemoteAddr().String()
		n.log.Info("net: accept connection", "addr", addr, "remoteAddr", remoteAddr)
		n.setTCPBuffersSize(n.log.WithDefaultArgs(context.Background(), "addr", addr, "remoteAdds", remoteAddr), conn)
		n.wg.Add(1)
		go func() {
			n.keepPeer(fmt.Sprintf("listen:%s:%s", uuid.Must(uuid.NewV7()).String(), remoteAddr), conn)
			defer n.wg.Done()
		}()
	}

	if l, ok := n.listens.LoadAndDelete(addr); ok {
		if err := l.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			n.log.Error("net: couldn't correct close listener", "addr", addr, "err", err)
		}
	}

	n.log.Info("net: listener closed", "addr", addr)
}

// keepPeer manages a single peer connection, handling read/write operations
// and cleanup when the connection is closed.
//
// ARCHITECTURE ROLE:
//   - Core peer lifecycle management function
//   - Creates and configures Peer instances with protocol handlers
//   - Manages peer connection lifecycle and error handling
//   - Integrates with the protocol layer via install/destroy callbacks
func (n *Net) keepPeer(name string, conn net.Conn) {
	peer := &Peer{
		inout:               n.onInstall(name),
		conn:                conn,
		writeTimeout:        n.writeTimeout,
		readAccumtTimeLimit: n.readAccumTimeLimit,
		bufferMaxSize:       n.bufferMaxSize,
		bufferMinToProcess:  n.bufferMinToProcess,
		writeBatchSize:      &utils.AvgVal{},
	}
	n.conns.Store(name, peer)

	readErr, writeErr, closeErr := peer.Keep(n.ctx)
	if readErr != nil {
		n.log.Error("net: couldn't read from peer", "name", name, "err", readErr, "trace_id", peer.GetTraceId())
	}
	if writeErr != nil {
		n.log.Error("net: couldn't write to peer", "name", name, "err", writeErr, "trace_id", peer.GetTraceId())
	}
	if closeErr != nil {
		n.log.Error("net: couldn't correct close peer", "name", name, "err", closeErr, "trace_id", peer.GetTraceId())
	}

	n.conns.Delete(name)
	peer.Close()
	n.onDestroy(name, peer)
}

// createListener creates a network listener based on the address scheme.
// Supports TCP, TLS, and QUIC (unimplemented) protocols.
func (n *Net) createListener(addr string) (net.Listener, error) {
	connType, address, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}

	var listener net.Listener
	switch connType {
	case TCP:
		config := net.ListenConfig{}
		if listener, err = config.Listen(n.ctx, "tcp", address); err != nil {
			return nil, err
		}

	case TLS:
		config := net.ListenConfig{}
		if listener, err = config.Listen(n.ctx, "tcp", address); err != nil {
			return nil, err
		}

		listener = tls.NewListener(listener, n.tlsConfig)

	case QUIC:
		return nil, errors.New("QUIC unimplemented")
	}

	return listener, nil
}

// createConn creates a network connection based on the address scheme.
// Supports TCP, TLS, and QUIC (unimplemented) protocols.
func (n *Net) createConn(addr string) (net.Conn, error) {
	connType, address, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}

	var conn net.Conn
	switch connType {
	case TCP:
		d := net.Dialer{Timeout: time.Minute}
		if conn, err = d.DialContext(n.ctx, "tcp", address); err != nil {
			return nil, err
		}

	case TLS:
		d := tls.Dialer{Config: n.tlsConfig}

		if conn, err = d.DialContext(n.ctx, "tcp", address); err != nil {
			return nil, err
		}

	case QUIC:
		return nil, errors.New("QUIC unimplemented")
	}

	return conn, err
}

// parseAddr parses a network address string and returns the connection type
// and address components. Supports URLs with schemes like "tcp://", "tls://", etc.
//
// Examples:
//   - "tcp://localhost:8080" -> TCP, "localhost:8080"
//   - "tls://example.com:443" -> TLS, "example.com:443"
//   - "localhost:8080" -> TCP, "localhost:8080"
func parseAddr(addr string) (ConnType, string, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return TCP, "", err
	}

	var conn ConnType

	switch u.Scheme {
	case "", "tcp", "tcp4", "tcp6":
		conn = TCP
	case "tls":
		conn = TLS
	case "quic":
		conn = QUIC
	default:
		return conn, addr, ErrAddressInvalid
	}

	u.Scheme = ""
	address := strings.TrimPrefix(u.String(), "//")

	return conn, address, nil
}
