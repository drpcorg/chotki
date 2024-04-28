package toytlv

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drpcorg/chotki/utils"
	"github.com/puzpuzpuz/xsync/v3"
	"golang.org/x/exp/constraints"
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

type Jack func(conn net.Conn) utils.FeedDrainCloser

// A TCP/TLS/QUIC server/client for the use case of real-time async communication.
// Differently from the case of request-response (like HTTP), we do not
// wait for a request, then dedicating a thread to processing, then sending
// back the resulting response. Instead, we constantly fan sendQueue tons of
// tiny messages. That dictates different work patterns than your typical
// HTTP/RPC server as, for example, we cannot let one slow receiver delay
// event transmission to all the other receivers.
type Transport struct {
	closed atomic.Bool

	wg   sync.WaitGroup
	jack Jack
	log  utils.Logger

	conns   *xsync.MapOf[string, *Peer]
	listens *xsync.MapOf[string, net.Listener]

	CertFile, KeyFile string
	ClientCertFiles   []string
}

func NewTransport(log utils.Logger, jack Jack) *Transport {
	return &Transport{
		log:     log,
		jack:    jack,
		conns:   xsync.NewMapOf[string, *Peer](),
		listens: xsync.NewMapOf[string, net.Listener](),
	}
}

func (t *Transport) Close() error {
	t.closed.Store(true)

	t.listens.Range(func(_ string, v net.Listener) bool {
		v.Close()
		return true
	})
	t.listens.Clear()

	t.conns.Range(func(_ string, p *Peer) bool {
		p.Close()
		return true
	})
	t.conns.Clear()

	t.wg.Wait()
	return nil
}

func (t *Transport) Connect(ctx context.Context, addr string) (err error) {
	// nil is needed so that Connect cannot be called
	// while KeepConnecting is connects
	if _, ok := t.conns.LoadOrStore(addr, nil); ok {
		return ErrAddressDuplicated
	}

	t.wg.Add(1)
	go func() {
		t.KeepConnecting(ctx, addr)
		t.wg.Done()
	}()

	return nil
}

func (de *Transport) Disconnect(addr string) (err error) {
	conn, ok := de.conns.LoadAndDelete(addr)
	if !ok {
		return ErrAddressUnknown
	}

	conn.Close()
	return nil
}

func (t *Transport) Listen(ctx context.Context, addr string) error {
	// nil is needed so that Listen cannot be called
	// while creating listener
	if _, ok := t.listens.LoadOrStore(addr, nil); ok {
		return ErrAddressDuplicated
	}

	listener, err := t.createListener(ctx, addr)
	if err != nil {
		t.listens.Delete(addr)
		return err
	}
	t.listens.Store(addr, listener)

	t.log.Debug("tlv: listening", "addr", addr)

	t.wg.Add(1)
	go func() {
		t.KeepListening(ctx, addr)
		t.wg.Done()
	}()

	return nil
}

func (de *Transport) Unlisten(addr string) error {
	listener, ok := de.listens.LoadAndDelete(addr)
	if !ok {
		return ErrAddressUnknown
	}

	return listener.Close()
}

func (t *Transport) KeepConnecting(ctx context.Context, addr string) {
	connBackoff := MIN_RETRY_PERIOD

	for !t.closed.Load() {
		select {
		case <-ctx.Done():
			break
		default:
			// continue
		}

		conn, err := t.createConn(ctx, addr)
		if err != nil {
			t.log.Error("couldn't connect", "addr", addr, "err", err)

			time.Sleep(connBackoff)
			connBackoff = min(MAX_RETRY_PERIOD, connBackoff*2)

			continue
		}

		t.log.Debug("tlv: connected", "addr", addr)

		connBackoff = MIN_RETRY_PERIOD
		t.keepPeer(ctx, addr, conn)
	}
}

func (t *Transport) KeepListening(ctx context.Context, addr string) {
	for !t.closed.Load() {
		select {
		case <-ctx.Done():
			break
		default:
			// continue
		}

		listener, ok := t.listens.Load(addr)
		if !ok {
			break
		}

		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}

			// reconnects are the client's problem, just continue
			t.log.Error("tlv: couldn't accept request", "addr", addr, "err", err)
			continue
		}

		remoteAddr := conn.RemoteAddr().String()
		t.log.Debug("tlv: accept connection", "addr", addr, "remoteAddr", remoteAddr)

		t.wg.Add(1)
		go func() {
			t.keepPeer(ctx, remoteAddr, conn)
			defer t.wg.Done()
		}()
	}

	if l, ok := t.listens.LoadAndDelete(addr); ok {
		if err := l.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.log.Error("tlv: couldn't correct close listener", "addr", addr, "err", err)
		}
	}

	t.log.Debug("tlv: listener closed", "addr", addr)
}

func (t *Transport) keepPeer(ctx context.Context, addr string, conn net.Conn) {
	peer := &Peer{inout: t.jack(conn), conn: conn}
	t.conns.Store(addr, peer)
	defer t.conns.Delete(addr)

	readErr, wrireErr, closeErr := peer.Keep(ctx)
	if readErr != nil {
		t.log.Error("tlv: couldn't read from peer", "addr", addr, "err", readErr)
	}
	if wrireErr != nil {
		t.log.Error("tlv: couldn't write to peer", "addr", addr, "err", wrireErr)
	}
	if closeErr != nil {
		t.log.Error("tlv: couldn't correct close peer", "addr", addr, "err", closeErr)
	}
}

func (t *Transport) createListener(ctx context.Context, addr string) (net.Listener, error) {
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

		tlsConfig, err := t.tlsConfig()
		if err != nil {
			return nil, err
		}

		listener = tls.NewListener(listener, tlsConfig)

	case QUIC:
		return nil, errors.New("QUIC unimplemented")
	}

	return listener, nil
}

func (t *Transport) createConn(ctx context.Context, addr string) (net.Conn, error) {
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
		var d tls.Dialer
		if d.Config, err = t.tlsConfig(); err != nil {
			return nil, err
		}
		if conn, err = d.DialContext(ctx, "tcp", address); err != nil {
			return nil, err
		}

	case QUIC:
		return nil, errors.New("QUIC unimplemented")
	}

	return conn, err
}

func (t *Transport) tlsConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
	}

	if len(t.ClientCertFiles) > 0 {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		tlsConfig.ClientCAs = x509.NewCertPool()

		for _, file := range t.ClientCertFiles {
			if clientCert, err := os.ReadFile(file); err != nil {
				return nil, err
			} else {
				tlsConfig.ClientCAs.AppendCertsFromPEM(clientCert)
			}
		}
	}

	return tlsConfig, nil
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

func min[T constraints.Ordered](s ...T) T {
	if len(s) == 0 {
		var zero T
		return zero
	}
	m := s[0]
	for _, v := range s {
		if m > v {
			m = v
		}
	}
	return m
}
