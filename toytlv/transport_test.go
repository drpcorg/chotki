package toytlv

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/assert"
)

// 1. create a server, create a client, echo
// 2. create a server, client, connect, disconn, reconnect
// 3. create a server, client, conn, stop the serv, relaunch, reconnect

type TestConsumer struct {
	rcvd utils.Records
	mx   sync.Mutex
	co   sync.Cond
}

func (c *TestConsumer) Drain(recs utils.Records) error {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.rcvd = append(c.rcvd, recs...)
	c.co.Broadcast()
	return nil
}

func (c *TestConsumer) Feed() (utils.Records, error) {
	c.mx.Lock()
	defer c.mx.Unlock()

	for len(c.rcvd) == 0 {
		c.co.Wait()
	}

	recs := c.rcvd
	c.rcvd = c.rcvd[len(c.rcvd):]
	return recs, nil
}

func (c *TestConsumer) Close() error {
	return nil
}

func TestTCPDepot_Connect(t *testing.T) {
	loop := "tcp://127.0.0.1:32000"

	cert, key := "testonly_cert.pem", "testonly_key.pem"
	log := utils.NewDefaultLogger(slog.LevelDebug)

	lCon := TestConsumer{}
	lCon.co.L = &lCon.mx
	l := NewTransport(log, func(conn net.Conn) utils.FeedDrainCloser {
		return &lCon
	})
	err := l.Listen(context.Background(), loop)
	assert.Nil(t, err)

	l.CertFile = cert
	l.KeyFile = key

	cCon := TestConsumer{}
	cCon.co.L = &cCon.mx
	c := NewTransport(log, func(conn net.Conn) utils.FeedDrainCloser {
		return &cCon
	})
	err = c.Connect(context.Background(), loop)
	assert.Nil(t, err)

	c.CertFile = cert
	c.KeyFile = key

	time.Sleep(time.Second * 2) // TODO: use events

	// send a record
	err = cCon.Drain(utils.Records{Record('M', []byte("Hi there"))})
	assert.Nil(t, err)

	rec, err := lCon.Feed()
	assert.Nil(t, err)
	assert.Greater(t, len(rec), 0)

	lit, body, rest := TakeAny(rec[0])
	assert.Equal(t, uint8('M'), lit)
	assert.Equal(t, "Hi there", string(body))
	assert.Equal(t, 0, len(rest))

	// respond to that
	err = lCon.Drain(utils.Records{Record('M', []byte("Re: Hi there"))})
	assert.Nil(t, err)

	rerec, err := cCon.Feed()
	assert.Nil(t, err)
	assert.Greater(t, len(rerec), 0)

	relit, rebody, rerest := TakeAny(rerec[0])
	assert.Equal(t, uint8('M'), relit)
	assert.Equal(t, "Re: Hi there", string(rebody))
	assert.Equal(t, 0, len(rerest))

	// cleanup
	err = c.Close()
	assert.Nil(t, err)

	err = l.Close()
	assert.Nil(t, err)
}
