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

	t.conns.Range(func(k string, v *Peer) bool {
		if err := v.Close(); err != nil {
			t.log.Error("couldn't close connection")
		}
		return true
	})

	t.listens.Range(func(k string, v net.Listener) bool {
		if err := v.Close(); err != nil {
			t.log.Error("couldn't close listener")
		}
		return true
	})

	t.conns.Clear()
	t.listens.Clear()

	t.wg.Wait()

	return nil
}

func (t *Transport) Connect(ctx context.Context, addr string) (err error) {
	if _, ok := t.conns.Load(addr); ok {
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
	connType, address, err := parseAddr(addr)
	if err != nil {
		return err
	}

	var listener net.Listener
	switch connType {
	case TCP:
		config := net.ListenConfig{}
		if listener, err = config.Listen(ctx, "tcp", address); err != nil {
			return err
		}

	case TLS:
		config := net.ListenConfig{}
		if listener, err = config.Listen(ctx, "tcp", address); err != nil {
			return err
		}

		tlsConfig, err := t.tlsConfig()
		if err != nil {
			return err
		}

		listener = tls.NewListener(listener, tlsConfig)

	case QUIC:
		return errors.New("QUIC unimplemented")
	}

	if _, ok := t.listens.LoadOrStore(addr, listener); ok {
		listener.Close()
		return ErrAddressDuplicated
	}

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
	conntime := time.Now()
	talkBackoff := MIN_RETRY_PERIOD
	connBackoff := MIN_RETRY_PERIOD

	if atLeast5min := conntime.Add(time.Minute * 5); atLeast5min.After(time.Now()) {
		talkBackoff = min(MAX_RETRY_PERIOD, talkBackoff*2)
	}

	for !t.closed.Load() {
		time.Sleep(connBackoff + talkBackoff)

		conn, err := t.createDialConnect(ctx, addr)
		if err != nil {
			t.log.Error("couldn't connect", "addr", addr, "err", err)
			connBackoff = min(MAX_RETRY_PERIOD, connBackoff*2)
			continue
		}

		connBackoff = MIN_RETRY_PERIOD

		peer := &Peer{inout: t.jack(conn)}
		peer.conn.Store(&conn)

		t.conns.Store(addr, peer)
		t.keepPeer(ctx, addr, peer)
	}
}

func (t *Transport) KeepListening(ctx context.Context, addr string) {
	for !t.closed.Load() {
		listener, ok := t.listens.Load(addr)
		if !ok {
			break
		}

		select {
		case <-ctx.Done():
			t.Close()
			break

		default:
			// continue
		}

		conn, err := listener.Accept()
		if err != nil {
			// reconnects are the client's responsibility, just skip
			t.log.Error("couldn't accept connect request", "err", err)
			continue
		}

		addr := conn.RemoteAddr().String()
		peer := &Peer{inout: t.jack(conn)}
		peer.conn.Store(&conn)

		t.conns.Store(addr, peer)
		go t.keepPeer(ctx, addr, peer)
	}
}

func (t *Transport) closePeer(addr string) error {
	if peer, ok := t.conns.LoadAndDelete(addr); ok {
		return peer.Close()
	}

	return nil
}

func (t *Transport) keepPeer(ctx context.Context, addr string, peer *Peer) error {
	t.wg.Add(1)
	defer t.wg.Done()

	rerrch := make(chan error)
	werrch := make(chan error)

	go func() {
		rerrch <- peer.KeepRead(ctx)
	}()
	go func() {
		werrch <- peer.KeepWrite(ctx)
	}()

	select {
	case err := <-rerrch:
		if err != nil {
			t.log.Error("couldn't read from peer", "addr", addr, "err", err)
			return t.closePeer(addr)
		}
	case err := <-werrch:
		if err != nil {
			t.log.Error("couldn't write to peer", "addr", addr, "err", err)
			return t.closePeer(addr)
		}
	}

	return nil
}

func (t *Transport) createDialConnect(ctx context.Context, addr string) (net.Conn, error) {
	connType, address, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}

	var conn net.Conn
	switch connType {
	case TCP:
		var d net.Dialer
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
