package utils

import (
	"errors"
	"io"
	"sync/atomic"
	"time"
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

type RecordQueue struct {
	closed  atomic.Bool
	timeout time.Duration
	ch      chan []byte
}

var ErrClosed = errors.New("[chotki] records queue is closed")

func NewRecordQueue(limit int, timeout time.Duration) *RecordQueue {
	return &RecordQueue{
		timeout: timeout,
		ch:      make(chan []byte, limit),
	}
}

func (q *RecordQueue) Close() error {
	q.closed.Store(true)
	close(q.ch)
	return nil
}

func (q *RecordQueue) Drain(recs Records) error {
	if closed := q.closed.Load(); closed {
		return ErrClosed
	}
	for _, pkg := range recs {
		q.ch <- pkg
	}
	return nil
}

func (q *RecordQueue) Feed() (recs Records, err error) {
	if closed := q.closed.Load(); closed {
		return nil, ErrClosed
	}

	select {
	case <-time.After(q.timeout):
		return Records{}, nil
	case pkg := <-q.ch:
		recs := Records{pkg}
		for ok := false; ok; {
			pkg, ok = <-q.ch
			recs = append(recs, pkg)
		}
		return recs, nil
	}
}
