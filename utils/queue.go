package utils

import (
	"errors"
	"io"
	"sync"
)

type Feeder interface {
	// Feed reads and returns records.
	// The EoF convention follows that of io.Reader:
	// can either return `records, EoF` or
	// `records, nil` followed by `nil/{}, EoF`
	Feed() (recs Records, err error)
}

type FeedCloser interface {
	Feeder
	io.Closer
}

type Drainer interface {
	Drain(recs Records) error
}

type DrainCloser interface {
	Drainer
	io.Closer
}

type FeedDrainCloser interface {
	Feeder
	Drainer
	io.Closer
}

// Records (a batch of) as a very universal primitive, especially
// for database/network op/packet processing. Batching allows
// for writev() and other performance optimizations. Also, if
// you have cryptography, blobs are way handier than structs.
// Records converts easily to net.Buffers.
type Records [][]byte

func (recs Records) recrem(total int64) (prelen int, prerem int64) {
	for len(recs) > prelen && int64(len(recs[prelen])) <= total {
		total -= int64(len(recs[prelen]))
		prelen++
	}
	prerem = total
	return
}

func (recs Records) WholeRecordPrefix(limit int64) (prefix Records, remainder int64) {
	prelen, remainder := recs.recrem(limit)
	prefix = recs[:prelen]
	return
}

func (recs Records) ExactSuffix(total int64) (suffix Records) {
	prelen, prerem := recs.recrem(total)
	suffix = recs[prelen:]
	if prerem != 0 { // damages the original, hence copy
		edited := make(Records, 1, len(suffix))
		edited[0] = suffix[0][prerem:]
		suffix = append(edited, suffix[1:]...)
	}
	return
}

func (recs Records) TotalLen() (total int64) {
	for _, r := range recs {
		total += int64(len(r))
	}
	return
}

type RecordQueue struct {
	recs  Records
	mu    sync.Mutex
	co    sync.Cond
	Limit int
}

var ErrWouldBlock = errors.New("the queue is over capacity")
var ErrClosed = errors.New("queue is closed")

func (q *RecordQueue) Drain(recs Records) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	was0 := len(q.recs) == 0
	if len(q.recs)+len(recs) > q.Limit {
		if q.Limit == 0 {
			return ErrClosed
		}
		return ErrWouldBlock
	}
	q.recs = append(q.recs, recs...)
	if was0 && q.co.L != nil {
		q.co.Broadcast()
	}
	return nil
}

func (q *RecordQueue) Close() error {
	q.Limit = 0
	return nil
}

func (q *RecordQueue) Feed() (recs Records, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.recs) == 0 {
		err = ErrWouldBlock
		if q.Limit == 0 {
			err = ErrClosed
		}
		return
	}
	wasfull := len(q.recs) >= q.Limit
	recs = q.recs
	q.recs = q.recs[len(q.recs):]
	if wasfull && q.co.L != nil {
		q.co.Broadcast()
	}
	return
}

func (q *RecordQueue) Blocking() FeedDrainCloser {
	if q.co.L == nil {
		q.co.L = &q.mu
	}
	return &blockingRecordQueue{q}
}

type blockingRecordQueue struct {
	queue *RecordQueue
}

func (bq *blockingRecordQueue) Close() error {
	return bq.queue.Close()
}

func (bq *blockingRecordQueue) Drain(recs Records) error {
	q := bq.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(recs) > 0 {
		was0 := len(q.recs) == 0
		for q.Limit <= len(q.recs) {
			if q.Limit == 0 {
				return ErrClosed
			}
			q.co.Wait()
		}
		qcap := q.Limit - len(q.recs)
		if qcap > len(recs) {
			qcap = len(recs)
		}
		q.recs = append(q.recs, recs[:qcap]...)
		recs = recs[qcap:]
		if was0 {
			q.co.Broadcast()
		}
	}
	return nil
}

func (bq *blockingRecordQueue) Feed() (recs Records, err error) {
	q := bq.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	wasfull := len(q.recs) >= q.Limit
	for len(q.recs) == 0 {
		if q.Limit == 0 {
			return nil, ErrClosed
		}
		q.co.Wait()
	}
	recs = q.recs
	q.recs = q.recs[len(q.recs):]
	if wasfull {
		q.co.Broadcast()
	}
	return
}

func Relay(feeder Feeder, drainer Drainer) error {
	recs, err := feeder.Feed()
	if err != nil {
		if len(recs) > 0 {
			_ = drainer.Drain(recs)
		}
		return err
	}
	err = drainer.Drain(recs)
	return err
}

func Pump(feeder Feeder, drainer Drainer) (err error) {
	for err == nil {
		err = Relay(feeder, drainer)
	}
	return
}

func PumpN(feeder Feeder, drainer Drainer, n int) (err error) {
	for err == nil && n > 0 {
		err = Relay(feeder, drainer)
		n--
	}
	return
}

func PumpThenClose(feed FeedCloser, drain DrainCloser) error {
	var ferr, derr error
	for ferr == nil && derr == nil {
		var recs Records
		recs, ferr = feed.Feed()
		if len(recs) > 0 { // e.g. Feed() may return data AND EOF
			derr = drain.Drain(recs)
		}
	}
	_ = feed.Close()
	_ = drain.Close()
	if ferr != nil {
		return ferr
	} else {
		return derr
	}
}
