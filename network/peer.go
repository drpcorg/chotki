package network

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/utils"
)

// Peer represents a single network connection with bidirectional communication capabilities.
// It manages the lifecycle of a connection, handling read/write operations with
// configurable buffering, batching, and timeout settings.
//
// Key Features:
//   - Concurrent read and write operations
//   - Configurable buffer sizes and processing thresholds
//   - Automatic batching of write operations
//   - Graceful connection cleanup
//   - Thread-safe state management
type Peer struct {
	closed         atomic.Bool
	wg             sync.WaitGroup
	writeBatchSize *utils.AvgVal

	conn                net.Conn
	inout               protocol.FeedDrainCloserTraced
	incomingBuffer      atomic.Int32
	readAccumtTimeLimit time.Duration
	bufferMaxSize       int
	bufferMinToProcess  int
	writeTimeout        time.Duration
}

// getReadTimeLimit returns the configured read time limit or a default value.
// This determines how long to wait for incoming data before processing the buffer.
func (p *Peer) getReadTimeLimit() time.Duration {
	if p.readAccumtTimeLimit != 0 {
		return p.readAccumtTimeLimit
	}
	return 5 * time.Second
}

// keepRead continuously reads data from the network connection and processes it.
// It implements a buffered reading strategy with configurable thresholds:
//   - Accumulates data until buffer size reaches bufferMinToProcess
//   - Processes data when read time limit is exceeded
//   - Handles incomplete protocol packets gracefully
//   - Uses goroutines for concurrent processing to avoid blocking reads
func (p *Peer) keepRead(ctx context.Context) error {
	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	readChannel := make(chan protocol.Records)
	errChannel := make(chan error)
	signal := make(chan struct{})
	defer close(readChannel)
	defer close(signal)
	go func() {
		defer close(errChannel)
		for ctx.Err() == nil {
			_, ok := <-signal
			if !ok {
				return
			}
			recs, ok := <-readChannel
			if !ok {
				return
			}
			if len(recs) == 0 {
				continue
			}
			if err := p.inout.Drain(ctx, recs); err != nil {
				select {
				case <-ctx.Done():
					return
				case errChannel <- err:
					return
				}
			}
		}
	}()
	var timelimit *time.Time
	for !p.closed.Load() {
		if len(errChannel) > 0 {
			return <-errChannel
		}
		if buf.Len() <= p.bufferMaxSize {
			if buf.Available() < TYPICAL_MTU {
				buf.Grow(TYPICAL_MTU)
			}

			idle := buf.AvailableBuffer()[:buf.Available()]
			if timelimit == nil {
				t := time.Now().Add(p.getReadTimeLimit())
				timelimit = &t
			}
			p.conn.SetReadDeadline(*timelimit)
			if n, err := p.conn.Read(idle); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, os.ErrDeadlineExceeded) {
					time.Sleep(time.Millisecond)
				} else {
					return err
				}
			} else {
				buf.Write(idle[:n])
			}
		}
		p.incomingBuffer.Store(int32(buf.Len()))

		if (timelimit != nil && time.Now().After(*timelimit)) || buf.Len() >= p.bufferMinToProcess || buf.Len() >= p.bufferMaxSize {
			select {
			case signal <- struct{}{}:
				recs, err := protocol.Split(&buf)
				if err != nil && !errors.Is(err, protocol.ErrIncomplete) {
					return err
				} else if errors.Is(err, protocol.ErrIncomplete) {
					if buf.Len() >= p.bufferMaxSize {
						return errors.Join(err, fmt.Errorf("buffer is not enough to read packet"))
					}
				}
				// this will allow us to start accumulate next buffer while processing previous one
				readChannel <- recs
				timelimit = nil
			case <-ctx.Done():
				return nil
			default:
			}
		}
	}

	return nil
}

func (p *Peer) GetTraceId() string {
	return p.inout.GetTraceId()
}

func (p *Peer) GetIncomingPacketBufferSize() int32 {
	return p.incomingBuffer.Load()
}

// keepWrite continuously writes data to the network connection.
// It retrieves data from the protocol layer via the Feed method and
// batches multiple records together for efficient network transmission.
// The method tracks batch sizes for monitoring and optimization purposes.
func (p *Peer) keepWrite(ctx context.Context) error {
	for !p.closed.Load() {
		select {
		case <-ctx.Done():
			break
		default:
			// continue
		}

		recs, err := p.inout.Feed(ctx)
		if err != nil {
			return err
		}
		batchSize := 0
		for _, r := range recs {
			batchSize += len(r)
		}
		p.writeBatchSize.Add(float64(batchSize))

		b := net.Buffers(recs)
		if p.writeTimeout != 0 {
			p.conn.SetWriteDeadline(time.Now().Add(p.writeTimeout))
		}
		for len(b) > 0 && err == nil {
			if _, err = b.WriteTo(p.conn); err != nil {
				return err
			}
		}
	}

	return nil
}

// Keep manages the main lifecycle of the peer connection, running both
// read and write operations concurrently. It returns three error values:
//   - rerr: error from the read operation
//   - werr: error from the write operation
//   - cerr: error from closing the connection
//
// The method ensures proper cleanup by closing the connection after
// the write operation completes, which will cancel any ongoing read operations.
func (p *Peer) Keep(ctx context.Context) (rerr, werr, cerr error) {
	p.wg.Add(2) // read & write
	defer p.wg.Add(-2)

	if p.closed.Load() {
		return nil, nil, nil
	}

	readErrCh, writeErrCh := make(chan error, 1), make(chan error, 1)
	go func() { readErrCh <- p.keepRead(ctx) }()
	go func() { writeErrCh <- p.keepWrite(ctx) }()

	for i := 0; i < 2; i++ {
		select {
		case rerr = <-readErrCh:
			if errors.Is(rerr, net.ErrClosed) {
				// That's ok, we probably close it ourselves.
				rerr = nil
			}
		case werr = <-writeErrCh:
			// You can't close it before it's written, but you can close it before it's read.
			// Close after the writing thread has finished, this will cancel all reading threads.
			cerr = p.conn.Close()
		}

		p.closed.Store(true)
	}
	p.conn = nil
	return
}

func (p *Peer) Close() {
	p.closed.Store(true)
	p.wg.Wait()

	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
	p.inout.Close()
}
