package toytlv

import (
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/drpcorg/chotki/toyqueue"
)

const (
	TYPICAL_MTU       = 1500
	MAX_OUT_QUEUE_LEN = 1 << 20 // 16MB of pointers is a lot

	MAX_RETRY_PERIOD = time.Minute
	MIN_RETRY_PERIOD = time.Second / 2
)

type TCPConn struct {
	addr      string
	conn      net.Conn
	inout     toyqueue.FeedDrainCloser
	wake      *sync.Cond
	outmx     sync.Mutex
	Reconnect bool
	KeepAlive bool
}

func (tcp *TCPConn) doRead() {
	err := tcp.read()
	if err != nil && err != ErrDisconnected {
		// TODO: error handling
		slog.Error("couldn't read from conn", "err", err)
	}
}

func (tcp *TCPConn) read() (err error) {
	var buf []byte
	conn := tcp.conn
	for conn != nil {
		buf, err = AppendRead(buf, conn, TYPICAL_MTU)
		if err != nil {
			break
		}
		var recs toyqueue.Records
		recs, buf, err = Split(buf)
		if len(recs) == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		if err != nil {
			break
		}

		err = tcp.inout.Drain(recs)
		if err != nil {
			break
		}

		conn = tcp.conn
	}

	if err != nil {
		// TODO: error handling
		slog.Error("couldn't read from conn", "err", err)
		tcp.Close()
	}
	return
}

func (tcp *TCPConn) doWrite() {
	conn := tcp.conn
	var err error
	var recs toyqueue.Records
	for conn != nil && err == nil {
		recs, err = tcp.inout.Feed()
		b := net.Buffers(recs)
		for len(b) > 0 && err == nil {
			_, err = b.WriteTo(conn)
		}
	}
	if err != nil {
		tcp.Close() // TODO err
	}
}

// Write what we believe is a valid ToyTLV frame.
// Provided for io.Writer compatibility
func (tcp *TCPConn) Write(data []byte) (n int, err error) {
	err = tcp.Drain(toyqueue.Records{data})
	if err == nil {
		n = len(data)
	}
	return
}

func (tcp *TCPConn) Drain(recs toyqueue.Records) (err error) {
	return tcp.inout.Drain(recs)
}

func (tcp *TCPConn) Feed() (recs toyqueue.Records, err error) {
	return tcp.inout.Feed()
}

func (tcp *TCPConn) KeepTalking() {
	talk_backoff := MIN_RETRY_PERIOD
	conn_backoff := MIN_RETRY_PERIOD

	for {
		conntime := time.Now()
		go tcp.doWrite()
		err := tcp.read()

		if !tcp.Reconnect {
			break
		}

		atLeast5min := conntime.Add(time.Minute * 5)
		if atLeast5min.After(time.Now()) {
			talk_backoff *= 2 // connected, tried to talk, failed => wait more
			if talk_backoff > MAX_RETRY_PERIOD {
				talk_backoff = MAX_RETRY_PERIOD
			}
		}

		for tcp.conn == nil {
			time.Sleep(conn_backoff + talk_backoff)
			tcp.conn, err = net.Dial("tcp", tcp.addr)
			if err != nil {
				conn_backoff = conn_backoff * 2
				if conn_backoff > MAX_RETRY_PERIOD/2 {
					conn_backoff = MAX_RETRY_PERIOD
				}
			} else {
				conn_backoff = MIN_RETRY_PERIOD
			}
		}
	}
}

func (tcp *TCPConn) Close() error {
	tcp.outmx.Lock()
	defer tcp.outmx.Unlock()

	// TODO writer closes on complete | 1 sec expired
	if tcp.conn != nil {
		if err := tcp.conn.Close(); err != nil {
			return err
		}

		tcp.conn = nil
		tcp.wake.Broadcast()
	}

	return nil
}

type Jack func(conn net.Conn) toyqueue.FeedDrainCloser

// A TCP server/client for the use case of real-time async communication.
// Differently from the case of request-response (like HTTP), we do not
// wait for a request, then dedicating a thread to processing, then sending
// back the resulting response. Instead, we constantly fan sendQueue tons of
// tiny messages. That dictates different work patterns than your typical
// HTTP/RPC server as, for example, we cannot let one slow receiver delay
// event transmission to all the other receivers.
type TCPDepot struct {
	conns   map[string]*TCPConn
	listens map[string]net.Listener
	conmx   sync.Mutex
	jack    Jack
}

func (de *TCPDepot) Open(jack Jack) {
	de.conmx.Lock()
	de.conns = make(map[string]*TCPConn)
	de.listens = make(map[string]net.Listener)
	de.conmx.Unlock()
	de.jack = jack
}

func (de *TCPDepot) Close() error {
	de.conmx.Lock()
	defer de.conmx.Unlock()

	for _, lstn := range de.listens {
		if err := lstn.Close(); err != nil {
			return err
		}
	}
	clear(de.listens)

	for _, con := range de.conns {
		if err := con.Close(); err != nil {
			return err
		}
	}
	clear(de.conns)

	return nil
}

// attrib?!
func (de *TCPDepot) Connect(addr string) (err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	peer := TCPConn{
		conn:  conn,
		addr:  addr,
		inout: de.jack(conn),
	}
	peer.wake = sync.NewCond(&peer.outmx)
	de.conmx.Lock()
	de.conns[addr] = &peer
	de.conmx.Unlock()

	go peer.KeepTalking()

	return nil
}

func (de *TCPDepot) DrainTo(recs toyqueue.Records, addr string) error {
	de.conmx.Lock()
	conn, ok := de.conns[addr]
	de.conmx.Unlock()
	if !ok {
		return ErrAddressUnknown
	}
	return conn.Drain(recs)
}

func (de *TCPDepot) Disconnect(addr string) (err error) {
	de.conmx.Lock()
	tcp, ok := de.conns[addr]
	de.conmx.Unlock()
	if !ok {
		return ErrAddressUnknown
	}
	tcp.Close()
	de.conmx.Lock()
	delete(de.conns, addr)
	de.conmx.Unlock()
	return nil
}

func (de *TCPDepot) Listen(addr string) (err error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}
	de.conmx.Lock()
	pre, ok := de.listens[addr]
	if ok {
		_ = pre.Close()
	}
	de.listens[addr] = listener
	de.conmx.Unlock()
	go de.KeepListening(addr)
	return
}

func (de *TCPDepot) StopListening(addr string) error {
	de.conmx.Lock()
	listener, ok := de.listens[addr]
	delete(de.listens, addr)
	de.conmx.Unlock()
	if !ok {
		return ErrAddressUnknown
	}
	return listener.Close()
}

func (de *TCPDepot) KeepListening(addr string) {
	for {
		de.conmx.Lock()
		listener, ok := de.listens[addr]
		de.conmx.Unlock()
		if !ok {
			break
		}
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		addr := conn.RemoteAddr().String()
		peer := TCPConn{
			conn:  conn,
			addr:  addr,
			inout: de.jack(conn),
		}
		peer.wake = sync.NewCond(&peer.outmx)
		de.conmx.Lock()
		de.conns[addr] = &peer
		de.conmx.Unlock()

		go peer.doWrite()
		go peer.doRead()
	}
}

func ReadBuf(buf []byte, rdr io.Reader) ([]byte, error) {
	avail := cap(buf) - len(buf)
	if avail < 512 {
		l := 4096
		if len(buf) > 2048 {
			l = len(buf) * 2
		}
		newbuf := make([]byte, l)
		copy(newbuf[:], buf)
		buf = newbuf[:len(buf)]
	}
	idle := buf[len(buf):cap(buf)]
	n, err := rdr.Read(idle)
	if err != nil {
		return buf, err
	}
	if n == 0 {
		return buf, io.EOF
	}
	buf = buf[:len(buf)+n]
	return buf, nil
}

func RoundPage(l int) int {
	if (l & 0xfff) != 0 {
		l = (l & ^0xfff) + 0x1000
	}
	return l
}

// AppendRead reads data from io.Reader into the *spare space* of the provided buffer,
// i.e. those cap(buf)-len(buf) vacant bytes. If the spare space is smaller than
// lenHint, allocates (as reading less bytes might be unwise).
func AppendRead(buf []byte, rdr io.Reader, lenHint int) ([]byte, error) {
	avail := cap(buf) - len(buf)
	if avail < lenHint {
		want := RoundPage(len(buf) + lenHint)
		newbuf := make([]byte, want)
		copy(newbuf[:], buf)
		buf = newbuf[:len(buf)]
	}
	idle := buf[len(buf):cap(buf)]
	n, err := rdr.Read(idle)
	if err != nil {
		return buf, err
	}
	if n == 0 {
		return buf, io.EOF
	}
	buf = buf[:len(buf)+n]
	return buf, nil
}
