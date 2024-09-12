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
	"sync/atomic"
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
	closed atomic.Bool

	wg        sync.WaitGroup
	log       utils.Logger
	onInstall InstallCallback
	onDestroy DestroyCallback

	conns   *xsync.MapOf[string, *Peer]
	listens *xsync.MapOf[string, net.Listener]

	TlsConfig *tls.Config
}

func NewNet(log utils.Logger, tlsConfig *tls.Config, install InstallCallback, destroy DestroyCallback) *Net {
	return &Net{
		log:       log,
		conns:     xsync.NewMapOf[string, *Peer](),
		listens:   xsync.NewMapOf[string, net.Listener](),
		onInstall: install,
		onDestroy: destroy,
		TlsConfig: tlsConfig,
	}
}

func (n *Net) Close() error {
	n.closed.Store(true)

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

func (n *Net) Connect(ctx context.Context, addr string) (err error) {
	return n.ConnectPool(ctx, addr, []string{addr})
}

func (n *Net) ConnectPool(ctx context.Context, name string, addrs []string) (err error) {
	// nil is needed so that Connect cannot be called
	// while KeepConnecting is connects
	if _, ok := n.conns.LoadOrStore(name, nil); ok {
		return ErrAddressDuplicated
	}

	n.wg.Add(1)
	go func() {
		n.KeepConnecting(ctx, fmt.Sprintf("connect:%s", name), addrs)
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

func (n *Net) Listen(ctx context.Context, addr string) error {
	// nil is needed so that Listen cannot be called
	// while creating listener
	if _, ok := n.listens.LoadOrStore(addr, nil); ok {
		return ErrAddressDuplicated
	}

	listener, err := n.createListener(ctx, addr)
	if err != nil {
		n.listens.Delete(addr)
		return err
	}
	n.listens.Store(addr, listener)

	n.log.Info("net: listening", "addr", addr)

	n.wg.Add(1)
	go func() {
		n.KeepListening(ctx, addr)
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

func (n *Net) KeepConnecting(ctx context.Context, name string, addrs []string) {
	connBackoff := MIN_RETRY_PERIOD

	for !n.closed.Load() {
		select {
		case <-ctx.Done():
			break
		default:
			// continue
		}

		var err error
		var conn net.Conn
		for _, addr := range addrs {
			conn, err = n.createConn(ctx, addr)
			if err == nil {
				break
			}
		}

		if err != nil {
			n.log.Error("net: couldn't connect", "name", name, "err", err)

			time.Sleep(connBackoff)
			connBackoff = min(MAX_RETRY_PERIOD, connBackoff*2)

			continue
		}

		n.log.Info("net: connected", "name", name)

		connBackoff = MIN_RETRY_PERIOD
		n.keepPeer(ctx, name, conn)
	}
}

func (n *Net) KeepListening(ctx context.Context, addr string) {
	for !n.closed.Load() {
		select {
		case <-ctx.Done():
			break
		default:
			// continue
		}

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

		n.wg.Add(1)
		go func() {
			n.keepPeer(ctx, fmt.Sprintf("listen:%s:%s", uuid.Must(uuid.NewV7()).String(), remoteAddr), conn)
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

func (n *Net) keepPeer(ctx context.Context, name string, conn net.Conn) {
	peer := &Peer{inout: n.onInstall(name), conn: conn}
	n.conns.Store(name, peer)

	readErr, wrireErr, closeErr := peer.Keep(ctx)
	if readErr != nil {
		n.log.Error("net: couldn't read from peer", "name", name, "err", readErr, "trace_id", peer.GetTraceId())
	}
	if wrireErr != nil {
		n.log.Error("net: couldn't write to peer", "name", name, "err", wrireErr, "trace_id", peer.GetTraceId())
	}
	if closeErr != nil {
		n.log.Error("net: couldn't correct close peer", "name", name, "err", closeErr, "trace_id", peer.GetTraceId())
	}

	n.conns.Delete(name)
	n.onDestroy(name, peer)
}

func (n *Net) createListener(ctx context.Context, addr string) (net.Listener, error) {
	connType, address, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}

	var listener net.Listener
	switch connType {
	case TCP:
		config := net.ListenConfig{}
		if listener, err = config.Listen(ctx, "tcp", address); err != nil {
			return nil, err
		}

	case TLS:
		config := net.ListenConfig{}
		if listener, err = config.Listen(ctx, "tcp", address); err != nil {
			return nil, err
		}

		listener = tls.NewListener(listener, n.TlsConfig)

	case QUIC:
		return nil, errors.New("QUIC unimplemented")
	}

	return listener, nil
}

func (n *Net) createConn(ctx context.Context, addr string) (net.Conn, error) {
	connType, address, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}

	var conn net.Conn
	switch connType {
	case TCP:
		d := net.Dialer{Timeout: time.Minute}
		if conn, err = d.DialContext(ctx, "tcp", address); err != nil {
			return nil, err
		}

	case TLS:
		d := tls.Dialer{Config: n.TlsConfig}

		if conn, err = d.DialContext(ctx, "tcp", address); err != nil {
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
