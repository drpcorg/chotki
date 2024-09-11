package utils

import (
	"context"
	"errors"
	"sync"
	"time"
)

type FDQueue[S ~[]E, E any] struct {
	ctx     context.Context
	close   context.CancelFunc
	timeout time.Duration
	ch      chan E
	active  sync.WaitGroup
}

var ErrClosed = errors.New("[chotki] feed/drain queue is closed")

func NewFDQueue[S ~[]E, E any](limit int, timeout time.Duration) *FDQueue[S, E] {
	ctx, cancel := context.WithCancel(context.Background())
	return &FDQueue[S, E]{
		timeout: timeout,
		ch:      make(chan E, limit),
		ctx:     ctx,
		close:   cancel,
	}
}

func (q *FDQueue[S, E]) Close() error {
	q.close()
	q.active.Wait()
	close(q.ch)
	q.ch = nil
	return nil
}

func (q *FDQueue[S, E]) Drain(ctx context.Context, recs S) error {
	if q.ctx.Err() != nil {
		return ErrClosed
	}
	q.active.Add(1)
	defer q.active.Done()
	for _, pkg := range recs {
		select {
		case <-ctx.Done():
			break
		case <-q.ctx.Done():
			break
		case q.ch <- pkg:
		}

	}
	return nil
}

func (q *FDQueue[S, E]) Feed(ctx context.Context) (recs S, err error) {
	if q.ctx.Err() != nil {
		return nil, ErrClosed
	}
	q.active.Add(1)
	defer q.active.Done()
	select {
	case <-q.ctx.Done():
		return
	case <-ctx.Done():
		return
	case <-time.After(q.timeout):
		return
	case pkg, ok := <-q.ch:
		if !ok {
			return
		}
		recs = append(recs, pkg)
		if len(q.ch) == 0 {
			return
		}
	}
	return
}
