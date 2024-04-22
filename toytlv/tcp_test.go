package toytlv

import (
	"github.com/drpcorg/chotki/toyqueue"
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"testing"
)

// 1. create a server, create a client, echo
// 2. create a server, client, connect, disconn, reconnect
// 3. create a server, client, conn, stop the serv, relaunch, reconnect

type TestConsumer struct {
	rcvd toyqueue.Records
	mx   sync.Mutex
	co   sync.Cond
}

func (c *TestConsumer) Drain(recs toyqueue.Records) error {
	c.mx.Lock()
	c.rcvd = append(c.rcvd, recs...)
	c.co.Signal()
	c.mx.Unlock()
	return nil
}

func (c *TestConsumer) Feed() (recs toyqueue.Records, err error) {
	c.mx.Lock()
	if len(c.rcvd) == 0 {
		c.co.Wait()
	}
	recs = c.rcvd
	c.rcvd = c.rcvd[len(c.rcvd):]
	c.mx.Unlock()
	return
}

func (c *TestConsumer) Close() error {
	return nil
}

func TestTCPDepot_Connect(t *testing.T) {

	loop := "127.0.0.1:12345"

	tc := TestConsumer{}
	tc.co.L = &tc.mx
	depot := TCPDepot{}
	addr := ""
	depot.Open(func(conn net.Conn) toyqueue.FeedDrainCloser {
		a := conn.RemoteAddr().String()
		if a != loop {
			addr = a
		}
		return &tc
	})

	err := depot.Listen(loop)
	assert.Nil(t, err)

	err = depot.Connect(loop)
	assert.Nil(t, err)

	// send a record
	recsto := toyqueue.Records{Record('M', []byte("Hi there"))}
	err = depot.DrainTo(recsto, loop)
	assert.Nil(t, err)
	rec, err := tc.Feed()
	assert.Nil(t, err)
	lit, body, rest := TakeAny(rec[0])
	assert.Equal(t, uint8('M'), lit)
	assert.Equal(t, "Hi there", string(body))
	assert.Equal(t, 0, len(rest))

	// respond to that
	recsback := toyqueue.Records{Record('M', []byte("Re: Hi there"))}
	err = depot.DrainTo(recsback, addr)
	assert.Nil(t, err)
	rerec, err := tc.Feed()
	assert.Nil(t, err)
	relit, rebody, rerest := TakeAny(rerec[0])
	assert.Equal(t, uint8('M'), relit)
	assert.Equal(t, "Re: Hi there", string(rebody))
	assert.Equal(t, 0, len(rerest))

	depot.Close()

}
