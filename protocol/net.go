package protocol

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

	"github.com/drpcorg/chotki/utils"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v3"
)

type ConnType = uint

const (
	TCP ConnType = iota + 1
	TLS
	QUIC
)

const (
	TYPICAL_MTU       = 1500
	MAX_OUT_QUEUE_LEN = 1 << 20 // 16MB of pointers is a lot

	MAX_RETRY_PERIOD = time.Minute
	MIN_RETRY_PERIOD = time.Second / 2
)

type InstallCallback func(name string) FeedDrainCloserTraced
type DestroyCallback func(name string, p Traced)

// A TCP/TLS/QUIC server/client for the use case of real-time async communication.
// Differently from the case of request-response (like HTTP), we do not
// wait for a request, then dedicating a thread to processing, then sending
// back the resulting response. Instead, we constantly fan sendQueue tons of
// tiny messages. That dictates different work patterns than your typical
// HTTP/RPC server as, for example, we cannot let one slow receiver delay
// event transmission to all the other receivers.
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
	bufferMaxSize      int
	bufferMinToProcess int
}

type NetOpt interface {
	Apply(*Net)
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
	ReadBuffers map[string]int32
}

func (n *Net) GetStats() NetStats {
	stats := NetStats{
		ReadBuffers: make(map[string]int32),
	}
	n.conns.Range(func(name string, peer *Peer) bool {
		if peer != nil {
			stats.ReadBuffers[name] = peer.GetIncomingPacketBufferSize()
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

func (n *Net) keepPeer(name string, conn net.Conn) {
	peer := &Peer{
		inout:               n.onInstall(name),
		conn:                conn,
		readAccumtTimeLimit: n.readAccumTimeLimit,
		bufferMaxSize:       n.bufferMaxSize,
		bufferMinToProcess:  n.bufferMinToProcess,
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
	n.onDestroy(name, peer)
}

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
