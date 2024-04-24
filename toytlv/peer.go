package toytlv

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/drpcorg/chotki/toyqueue"
)

type Peer struct {
	conn  atomic.Pointer[net.Conn]
	inout toyqueue.FeedDrainCloser
}

func (p *Peer) KeepRead(ctx context.Context) error {
	var err error
	var buf []byte
	var recs toyqueue.Records

	for {
		select {
		case <-ctx.Done():
			break
		default:
			// continue
		}

		conn := p.conn.Load()
		if conn == nil {
			break
		}

		if buf, err = appendRead(buf, *conn, TYPICAL_MTU); err != nil {
			if errors.Is(err, io.EOF) {
				time.Sleep(time.Millisecond)
				continue
			}

			return err
		}

		recs, buf, err = Split(buf)
		if err != nil {
			return err
		} else if len(recs) == 0 {
			time.Sleep(time.Millisecond)
			continue
		}

		if err = p.inout.Drain(recs); err != nil {
			return err
		}
	}

	return nil
}

func (p *Peer) KeepWrite(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			break
		default:
			// continue
		}

		conn := p.conn.Load()
		if conn == nil {
			break
		}

		recs, err := p.inout.Feed()
		if err != nil {
			return err
		}

		b := net.Buffers(recs)
		for len(b) > 0 && err == nil {
			// TODO: consider the number of bytes written
			if _, err = b.WriteTo(*conn); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *Peer) Close() error {
	// TODO writer closes on complete | 1 sec expired
	if conn := p.conn.Swap(nil); conn != nil {
		if err := (*conn).Close(); err != nil {
			return err
		}
	}

	return nil
}

func roundPage(l int) int {
	if (l & 0xfff) != 0 {
		l = (l & ^0xfff) + 0x1000
	}
	return l
}

// appendRead reads data from io.Reader into the *spare space* of the provided buffer,
// i.e. those cap(buf)-len(buf) vacant bytes. If the spare space is smaller than
// lenHint, allocates (as reading less bytes might be unwise).
func appendRead(buf []byte, rdr io.Reader, lenHint int) ([]byte, error) {
	avail := cap(buf) - len(buf)
	if avail < lenHint {
		want := roundPage(len(buf) + lenHint)
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
