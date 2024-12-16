package utils

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type FDQueue[S ~[]E, E any] struct {
	ctx        context.Context
	close      context.CancelFunc
	timelimit  time.Duration
	ch         chan E
	active     sync.WaitGroup
	batchSize  int
	overflowed atomic.Bool
}

var ErrClosed = errors.New("[chotki] feed/drain queue is closed")

func NewFDQueue[S ~[]E, E any](limit int, timelimit time.Duration, batchSize int) *FDQueue[S, E] {
	ctx, cancel := context.WithCancel(context.Background())
	return &FDQueue[S, E]{
		timelimit: timelimit,
		ch:        make(chan E, limit),
		ctx:       ctx,
		close:     cancel,
		batchSize: batchSize,
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
	if q.ctx.Err() != nil || q.overflowed.Load() {
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
		case <-time.After(q.timelimit):
			q.overflowed.Store(true)
		case q.ch <- pkg:
		}

	}
	return nil
}

func (q *FDQueue[S, E]) Feed(ctx context.Context) (recs S, err error) {
	if q.ctx.Err() != nil || q.overflowed.Load() {
		return nil, ErrClosed
	}
	q.active.Add(1)
	defer q.active.Done()
	timelimit := time.After(q.timelimit)
	for {
		select {
		case <-q.ctx.Done():
			return
		case <-ctx.Done():
			return
		case <-timelimit:
			return
		case pkg, ok := <-q.ch:
			if !ok {
				return
			}
			recs = append(recs, pkg)
			if len(recs) > q.batchSize {
				return
			}
		}
	}
}
