package utils

import (
	"errors"
	"sync/atomic"
	"time"
)

type FDQueue[S ~[]E, E any] struct {
	closed  atomic.Bool
	timeout time.Duration
	ch      chan E
}

var ErrClosed = errors.New("[chotki] feed/drain queue is closed")

func NewFDQueue[S ~[]E, E any](limit int, timeout time.Duration) *FDQueue[S, E] {
	return &FDQueue[S, E]{
		timeout: timeout,
		ch:      make(chan E, limit),
	}
}

func (q *FDQueue[S, E]) Close() error {
	q.closed.Store(true)
	close(q.ch)
	return nil
}

func (q *FDQueue[S, E]) Drain(recs S) error {
	if closed := q.closed.Load(); closed {
		return ErrClosed
	}
	for _, pkg := range recs {
		q.ch <- pkg
	}
	return nil
}

func (q *FDQueue[S, E]) Feed() (recs S, err error) {
	if closed := q.closed.Load(); closed {
		return nil, ErrClosed
	}

	select {
	case <-time.After(q.timeout):
		return
	case pkg := <-q.ch:
		recs = append(recs, pkg)
		for ok := false; ok; {
			pkg, ok = <-q.ch
			recs = append(recs, pkg)
		}
		return
	}
}
