package toyqueue

import (
	"errors"
	"sync"
)

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
	lock  sync.Mutex
	cond  sync.Cond
	Limit int
}

var ErrWouldBlock = errors.New("the queue is over capacity")
var ErrClosed = errors.New("queue is closed")

func (q *RecordQueue) Drain(recs Records) error {
	q.lock.Lock()
	was0 := len(q.recs) == 0
	if len(q.recs)+len(recs) > q.Limit {
		q.lock.Unlock()
		if q.Limit == 0 {
			return ErrClosed
		}
		return ErrWouldBlock
	}
	q.recs = append(q.recs, recs...)
	if was0 && q.cond.L != nil {
		q.cond.Broadcast()
	}
	q.lock.Unlock()
	return nil
}

func (q *RecordQueue) Close() error {
	q.Limit = 0
	return nil
}

func (q *RecordQueue) Feed() (recs Records, err error) {
	q.lock.Lock()
	if len(q.recs) == 0 {
		err = ErrWouldBlock
		if q.Limit == 0 {
			err = ErrClosed
		}
		q.lock.Unlock()
		return
	}
	wasfull := len(q.recs) >= q.Limit
	recs = q.recs
	q.recs = q.recs[len(q.recs):]
	if wasfull && q.cond.L != nil {
		q.cond.Broadcast()
	}
	q.lock.Unlock()
	return
}

func (q *RecordQueue) Blocking() FeedDrainCloser {
	if q.cond.L == nil {
		q.cond.L = &q.lock
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
	q.lock.Lock()
	for len(recs) > 0 {
		was0 := len(q.recs) == 0
		for q.Limit <= len(q.recs) {
			if q.Limit == 0 {
				q.lock.Unlock()
				return ErrClosed
			}
			q.cond.Wait()
		}
		qcap := q.Limit - len(q.recs)
		if qcap > len(recs) {
			qcap = len(recs)
		}
		q.recs = append(q.recs, recs[:qcap]...)
		recs = recs[qcap:]
		if was0 {
			q.cond.Broadcast()
		}
	}
	q.lock.Unlock()
	return nil
}

func (bq *blockingRecordQueue) Feed() (recs Records, err error) {
	q := bq.queue
	q.lock.Lock()
	wasfull := len(q.recs) >= q.Limit
	for len(q.recs) == 0 {
		if q.Limit == 0 {
			q.lock.Unlock()
			return nil, ErrClosed
		}
		q.cond.Wait()
	}
	recs = q.recs
	q.recs = q.recs[len(q.recs):]
	if wasfull {
		q.cond.Broadcast()
	}
	q.lock.Unlock()
	return
}
