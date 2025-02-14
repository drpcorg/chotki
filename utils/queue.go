package utils

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

type accumulator[T ~[][]byte] struct {
	data T
	size int
}

type FDQueue[T ~[][]byte] struct {
	ctx        context.Context
	close      context.CancelFunc
	timelimit  time.Duration
	batchSize  int
	accum      atomic.Pointer[accumulator[T]]
	maxSize    int
	overflowed atomic.Bool

	readLock  chan struct{}
	writeLock chan struct{}
	syncLock  chan struct{}

	writeSignal atomic.Pointer[chan struct{}]
	readSignal  atomic.Pointer[chan struct{}]
}

var ErrClosed = errors.New("[chotki] feed/drain queue is closed")
var ErrOverflow = errors.New("[chotki] feed/drain queue is overflowed")

func NewFDQueue[T ~[][]byte](limit int, timelimit time.Duration, batchSize int) *FDQueue[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &FDQueue[T]{
		timelimit: timelimit,
		ctx:       ctx,
		close:     cancel,
		maxSize:   limit,
		batchSize: batchSize,
		readLock:  make(chan struct{}, 1),
		writeLock: make(chan struct{}, 1),
		syncLock:  make(chan struct{}, 1),
	}
}

func (q *FDQueue[T]) Close() error {
	q.close()
	q.accum.Store(nil)
	return nil
}

func (q *FDQueue[T]) Size() int {
	if q.ctx.Err() != nil {
		return 0
	}
	if q.accum.Load() != nil {
		return q.accum.Load().size
	}
	return 0
}

type wait int

const (
	waitTimeout wait = iota
	waitOK
	waitCanceled
)

func (q *FDQueue[T]) lock(timer *time.Timer, lock chan struct{}, ctx context.Context) wait {
	select {
	case lock <- struct{}{}:
		return waitOK
	case <-q.ctx.Done():
		return waitCanceled
	case <-ctx.Done():
		return waitCanceled
	case <-timer.C:
		return waitTimeout
	}
}

func (q *FDQueue[T]) waitSignal(timer *time.Timer, signal chan struct{}, ctx context.Context) wait {
	select {
	case <-signal:
		return waitOK
	case <-q.ctx.Done():
		return waitCanceled
	case <-ctx.Done():
		return waitCanceled
	case <-timer.C:
		return waitTimeout
	}
}

func (q *FDQueue[T]) Drain(ctx context.Context, recs T) error {
	if q.ctx.Err() != nil {
		return ErrClosed
	}
	if q.overflowed.Load() {
		return ErrOverflow
	}

	timer := time.NewTimer(q.timelimit)
	defer timer.Stop()

	// try aquire write lock, so all writes would be ordered
	switch q.lock(timer, q.writeLock, ctx) {
	case waitCanceled:
		return nil
	case waitTimeout:
		q.overflowed.Store(true)
		return ErrOverflow
	case waitOK:
		defer func() { <-q.writeLock }()
	}

	// try to aquire sync lock, syncronizes reads and writes
	switch q.lock(timer, q.syncLock, ctx) {
	case waitCanceled:
		return nil
	case waitTimeout:
		q.overflowed.Store(true)
		return ErrOverflow
	case waitOK:
	}

	for len(recs) > 0 {
		accumPointer := q.accum.Load()
		accum := accumPointer
		if accum == nil {
			accum = &accumulator[T]{
				data: nil,
				size: 0,
			}
		}
		freespace := q.maxSize - accum.size
		writeSize := 0
		towrite := 0
		for _, pkg := range recs {
			length := len(pkg)
			if length <= freespace {
				towrite++
				freespace -= length
				writeSize += length
			} else {
				break
			}
		}
		needwait := false
		if towrite > 0 {
			accum = &accumulator[T]{
				data: append(accum.data, recs[:towrite]...),
				size: accum.size + writeSize,
			}
			if q.accum.CompareAndSwap(accumPointer, accum) {
				recs = recs[towrite:]
				// if there is a read waiting, signal it to wake up
				signal := q.readSignal.Swap(nil)
				if signal != nil {
					*signal <- struct{}{}
				}
				if len(recs) > 0 {
					needwait = true
				}
			}
		} else {
			needwait = true
		}
		if needwait {
			// creating signaling channel, to wait on it
			signal := make(chan struct{}, 1)
			q.writeSignal.Store(&signal)
			// releasing sync lock, to allow reads
			<-q.syncLock
			// now waiting some read to signal us through write signal
			switch q.waitSignal(timer, signal, ctx) {
			case waitOK:
				// aquiring sync lock again
				switch q.lock(timer, q.syncLock, ctx) {
				case waitOK:
				case waitCanceled:
					return nil
				case waitTimeout:
					q.overflowed.Store(true)
					return ErrOverflow
				}
			case waitCanceled:
				return nil
			case waitTimeout:
				q.overflowed.Store(true)
				return ErrOverflow
			}
		}
	}
	<-q.syncLock
	return nil
}

func (q *FDQueue[T]) Feed(ctx context.Context) (recs T, err error) {
	if q.ctx.Err() != nil {
		return nil, ErrClosed
	}
	if q.overflowed.Load() {
		return nil, ErrOverflow
	}

	timer := time.NewTimer(q.timelimit)
	defer timer.Stop()

	// try aquire read lock, so all reads would be ordered
	switch q.lock(timer, q.readLock, ctx) {
	case waitCanceled, waitTimeout:
		return
	case waitOK:
		defer func() { <-q.readLock }()
	}

	// try to aquire sync lock, syncronizes reads and writes
	switch q.lock(timer, q.syncLock, ctx) {
	case waitCanceled, waitTimeout:
		return
	case waitOK:
	}

	payloadSize := 0
	for {
		data := q.accum.Load()
		needwait := false
		iterPayloadSize := 0
		if data != nil {
			read := 0
			for _, pkg := range data.data {
				recs = append(recs, pkg)
				payloadSize += len(pkg)
				iterPayloadSize += len(pkg)
				read++
				if payloadSize >= q.batchSize {
					break
				}
			}
			// we have sync lock here so its safe
			data.data = data.data[read:]
			data.size = data.size - iterPayloadSize

			// if a write waits us, signal it
			signal := q.writeSignal.Swap(nil)
			if signal != nil {
				*signal <- struct{}{}
			}

			// we have enough data, release lock and return
			if payloadSize >= q.batchSize {
				<-q.syncLock
				return recs, nil
			} else {
				needwait = true
			}
		} else {
			needwait = true
		}
		if needwait {
			// creating signal channel to wait on
			signal := make(chan struct{}, 1)
			q.readSignal.Store(&signal)
			// releasing sync lock, to allow writes
			<-q.syncLock
			select {
			case <-signal:
				// aquiring sync lock again
				switch q.lock(timer, q.syncLock, ctx) {
				case waitCanceled, waitTimeout:
					return
				case waitOK:
				}
			case <-q.ctx.Done():
				return
			case <-ctx.Done():
				return
			case <-timer.C:
				return
			}
		}
	}
}
