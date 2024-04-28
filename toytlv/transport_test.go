package toytlv

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/assert"
)

// 1. create a server, create a client, echo
// 2. create a server, client, connect, disconn, reconnect
// 3. create a server, client, conn, stop the serv, relaunch, reconnect

func TestTCPDepot_Connect(t *testing.T) {
	loop := "tcp://127.0.0.1:32000"

	cert, key := "testonly_cert.pem", "testonly_key.pem"
	log := utils.NewDefaultLogger(slog.LevelDebug)

	lCon := utils.NewRecordQueue(0, time.Millisecond)
	l := NewTransport(log, func(conn net.Conn) utils.FeedDrainCloser {
		return lCon
	})
	err := l.Listen(context.Background(), loop)
	assert.Nil(t, err)

	l.CertFile = cert
	l.KeyFile = key

	cCon := utils.NewRecordQueue(0, time.Millisecond)
	c := NewTransport(log, func(conn net.Conn) utils.FeedDrainCloser {
		return cCon
	})
	err = c.Connect(context.Background(), loop)
	assert.Nil(t, err)

	c.CertFile = cert
	c.KeyFile = key

	// Wait connection, todo use events
	time.Sleep(time.Second * 2)

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
