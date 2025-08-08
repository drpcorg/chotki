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

// Peer implements a high-performance bidirectional network connection handler for the Chotki protocol.
// It provides asynchronous, buffered I/O with intelligent batching strategies to minimize syscalls
// and maximize throughput while maintaining protocol integrity.
//
// Architecture:
//   - Separates read/write operations into independent goroutines for full duplex communication
//   - Uses adaptive buffering with MTU-aligned growth (1500 bytes) to optimize network utilization
//   - Implements protocol-aware packet boundary detection to handle TLV record fragmentation
//   - Employs lock-free atomic operations for thread-safe state management and metrics collection
//
// Flow Control:
//   - Read path: Accumulates data until bufferMinToProcess threshold or readAccumtTimeLimit timeout
//   - Write path: Batches multiple protocol records using vectored I/O (WriteTo) for efficiency
//   - Processing pipeline: Concurrent record parsing to prevent read stalls during heavy processing
//
// The FeedDrainCloserTraced interface bridges network transport with protocol processing layers,
// providing Feed() for outbound records and Drain() for inbound record consumption.
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

func (p *Peer) getReadTimeLimit() time.Duration {
	if p.readAccumtTimeLimit != 0 {
		return p.readAccumtTimeLimit
	}
	return 5 * time.Second
}

// keepRead implements the asynchronous read path with intelligent buffering and concurrent processing.
//
// Buffer Management Strategy:
//   - Maintains a growing bytes.Buffer that auto-expands in TYPICAL_MTU (1500 byte) chunks
//   - Enforces bufferMaxSize limit to prevent unbounded memory growth
//   - Uses SetReadDeadline() with adaptive timeouts to balance latency vs throughput
//
// Processing Pipeline:
//   - Spawns dedicated goroutine for record processing to prevent I/O stalls
//   - Coordinates via channels: signal triggers processing, readChannel carries parsed records
//   - Handles protocol.ErrIncomplete for fragmented TLV records, buffering until complete
//   - Updates atomic incomingBuffer counter for monitoring and flow control
//
// Termination Conditions:
//   - bufferMinToProcess bytes accumulated (immediate processing for large batches)
//   - readAccumtTimeLimit timeout exceeded (latency protection)
//   - bufferMaxSize reached (backpressure protection)
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

// keepWrite implements the asynchronous write path with vectored I/O optimization.
//
// Performance Optimizations:
//   - Uses net.Buffers for vectored writes (WriteTo), reducing syscall overhead
//   - Batches multiple protocol records in single network operations
//   - Applies configurable write timeouts via SetWriteDeadline() to prevent hangs
//   - Tracks batch size metrics via writeBatchSize for throughput monitoring
//
// The method continuously polls inout.Feed() for outbound records, aggregating
// them into network-efficient batches. Each batch size is measured in bytes
// and recorded for performance analysis and capacity planning.
func (p *Peer) keepWrite(ctx context.Context) error {
	for !p.closed.Load() {
		select {
		case <-ctx.Done():
			return nil
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

// Keep orchestrates the full-duplex peer lifecycle with coordinated error handling.
//
// Concurrency Model:
//   - Launches independent keepRead() and keepWrite() goroutines for parallel I/O
//   - Uses buffered error channels to capture termination conditions from either path
//   - Implements "write-first shutdown" pattern: write completion triggers connection close
//
// Error Semantics:
//   - rerr: read path errors (filtered to ignore expected net.ErrClosed from shutdown)
//   - werr: write path errors (triggers immediate connection termination)
//   - cerr: connection close errors (resource cleanup failures)
//
// Shutdown Sequence:
//  1. Write goroutine completes/fails → close connection → triggers read cancellation
//  2. Read goroutine handles net.ErrClosed gracefully as expected shutdown signal
//  3. Both goroutines terminate → final cleanup sets conn=nil
//
// This ensures writes complete before reads are cancelled, maintaining protocol coherence.
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
