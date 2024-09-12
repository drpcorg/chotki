package protocol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/assert"
)

// 1. create a server, create a client, echo
// 2. create a server, client, connect, disconn, reconnect
// 3. create a server, client, conn, stop the serv, relaunch, reconnect

func tlsConfig(servername string) *tls.Config {
	const (
		serverCertFile = "testdata/server_cert.pem"    // contains the server's certificate (public key).
		serverKeyFile  = "testdata/server_key.pem"     // contains the server's private key.
		serverCAFile   = "testdata/server_ca_cert.pem" // contains the certificate of the certificate authority that can verify the server's certificate.

		clientCertFile = "testdata/client_cert.pem"    // contains the client's certificate (public key).
		clientKeyFile  = "testdata/client_key.pem"     // contains the client's private key.
		clientCAFile   = "testdata/client_ca_cert.pem" // contains the certificate of the certificate authority that can verify the client's certificate.
	)

	// setup for listening
	serverCert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	if err != nil {
		log.Fatalf("failed to load key pair: %s", err)
	}

	clientCAs := x509.NewCertPool()
	if caBytes, err := os.ReadFile(clientCAFile); err != nil {
		log.Fatalf("failed to read ca cert %q: %v", clientCAFile, err)
	} else if ok := clientCAs.AppendCertsFromPEM(caBytes); !ok {
		log.Fatalf("failed to parse %q", clientCAFile)
	}

	// setup for connection
	clientCert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		log.Fatalf("failed to load client cert: %v", err)
	}

	rootCAs := x509.NewCertPool()
	if caBytes, err := os.ReadFile(serverCAFile); err != nil {
		log.Fatalf("failed to read ca cert %q: %v", serverCAFile, err)
	} else if ok := rootCAs.AppendCertsFromPEM(caBytes); !ok {
		log.Fatalf("failed to parse %q", serverCAFile)
	}

	return &tls.Config{
		ServerName:   servername,
		RootCAs:      rootCAs,
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{serverCert, clientCert},
	}
}

type TracedQueue[S ~[]E, E any] struct {
	*utils.FDQueue[S, E]
}

func (t *TracedQueue[S, E]) GetTraceId() string {
	return ""
}

func TestTCPDepot_Connect(t *testing.T) {
	loop := "tls://127.0.0.1:32000"

	log := utils.NewDefaultLogger(slog.LevelDebug)

	lCon := utils.NewFDQueue[Records](16, time.Millisecond)
	l := NewNet(log, nil, func(_ string) FeedDrainCloserTraced {
		return &TracedQueue[Records, []byte]{lCon}
	}, func(_ string, t Traced) { lCon.Close() })
	l.TlsConfig = tlsConfig("a.chotki.local")

	err := l.Listen(context.Background(), loop)
	assert.Nil(t, err)

	cCon := utils.NewFDQueue[Records](16, time.Millisecond)
	c := NewNet(log, nil, func(_ string) FeedDrainCloserTraced {
		return &TracedQueue[Records, []byte]{cCon}
	}, func(_ string, t Traced) { cCon.Close() })
	c.TlsConfig = tlsConfig("b.chotki.local")

	err = c.Connect(context.Background(), loop)
	assert.Nil(t, err)
	time.Sleep(time.Second) // Wait connection, todo use events

	// send a record
	err = cCon.Drain(context.Background(), Records{Record('M', []byte("Hi there"))})
	assert.Nil(t, err)

	rec, err := lCon.Feed(context.Background())
	assert.Nil(t, err)
	assert.Greater(t, len(rec), 0)

	lit, body, rest := TakeAny(rec[0])
	assert.Equal(t, uint8('M'), lit)
	assert.Equal(t, "Hi there", string(body))
	assert.Equal(t, 0, len(rest))

	// respond to that
	err = lCon.Drain(context.Background(), Records{Record('M', []byte("Re: Hi there"))})
	assert.Nil(t, err)

	rerec, err := cCon.Feed(context.Background())
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

func TestTCPDepot_ConnectFailed(t *testing.T) {
	loop := "tls://127.0.0.1:32000"

	log := utils.NewDefaultLogger(slog.LevelDebug)

	cCon := utils.NewFDQueue[Records](16, time.Millisecond)
	c := NewNet(log, nil, func(_ string) FeedDrainCloserTraced {
		return &TracedQueue[Records, []byte]{cCon}
	}, func(_ string, t Traced) { cCon.Close() })
	c.TlsConfig = tlsConfig("b.chotki.local")

	err := c.Connect(context.Background(), loop)
	assert.Nil(t, err)
	time.Sleep(time.Second) // Wait connection, todo use events

	// cleanup
	err = c.Close()
	assert.Nil(t, err)
}
