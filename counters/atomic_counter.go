// Package counters provides AtomicCounter, a lock-free CRDT counter (N or Z).
// Get/Increment are lock-free; the manager periodically flushes mine and reloads theirs.
// Increments since last flush are lost on hard crash — deliberate tradeoff for a lock-free hot path.
package counters

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/rdx"
)

var ErrNotCounter error = fmt.Errorf("not a counter")
var ErrCounterNotLoaded error = fmt.Errorf("counter not loaded")
var ErrDecrementN error = fmt.Errorf("decrementing natural counter")

// AtomicCounter is the in-memory state for one (rid, offset) counter field; Increment/Get are lock-free.
type AtomicCounter struct {
	data     any    // *nState | *zState; set once on first successful load
	rid      rdx.ID
	offset   uint64
	db       host.Host
	loaded   atomic.Bool
	accessed atomic.Bool // set by Get/Increment to request a reload on the next background tick
}

type nState struct {
	mine, theirs atomic.Int64
	lastSynced   int64
}

type zState struct {
	mine, theirs atomic.Int64
	lastSynced   int64
	rev          int64
}

func newAtomicCounter(db host.Host, rid rdx.ID, offset uint64) *AtomicCounter {
	return &AtomicCounter{db: db, rid: rid, offset: offset}
}

// load refreshes theirs from DB; on first call sets counter kind and mine/lastSynced baseline.
// Mutex-free: caller holds the manager mutex.
func (a *AtomicCounter) load() error {
	rdt, tlv, err := a.db.ObjectFieldTLV(a.rid.ToOff(a.offset))
	if err != nil {
		return err
	}
	first := !a.loaded.Load()
	if first {
		switch rdt {
		case rdx.Natural:
			a.data = &nState{}
		case rdx.ZCounter:
			a.data = &zState{}
		default:
			return ErrNotCounter
		}
	}
	switch c := a.data.(type) {
	case *nState:
		sum, mine := rdx.Nnative2(tlv, a.db.Source())
		c.theirs.Store(int64(sum) - int64(mine))
		if first {
			c.mine.Store(int64(mine))
			c.lastSynced = int64(mine)
		}
	case *zState:
		sum, mine, rev := rdx.Znative3(tlv, a.db.Source())
		c.theirs.Store(sum - mine)
		if first {
			c.mine.Store(mine)
			c.lastSynced = mine
			c.rev = rev
		}
	default:
		return ErrNotCounter
	}
	a.loaded.Store(true) // publish baseline before any lock-free op
	return nil
}

// pendingFlush returns the field-edit op if mine changed since last flush (bumps Z rev); changed=false means nothing to do.
// Does NOT commit — manager batches via CommitBatch and calls markSynced on success. Mutex-free: caller holds manager mutex.
func (a *AtomicCounter) pendingFlush() (changed bool, rdt byte, op []byte, syncedTo int64) {
	switch c := a.data.(type) {
	case *nState:
		m := c.mine.Load()
		if m == c.lastSynced {
			return false, 0, nil, 0
		}
		return true, rdx.Natural, rdx.Ntlvt(uint64(m), a.db.Source()), m
	case *zState:
		m := c.mine.Load()
		if m == c.lastSynced {
			return false, 0, nil, 0
		}
		c.rev++
		return true, rdx.ZCounter, rdx.Ztlvt(m, a.db.Source(), c.rev), m
	}
	return false, 0, nil, 0
}

// markSynced records that contributions up to syncedTo are persisted. Mutex-free: caller holds manager mutex.
func (a *AtomicCounter) markSynced(syncedTo int64) {
	switch c := a.data.(type) {
	case *nState:
		c.lastSynced = syncedTo
	case *zState:
		c.lastSynced = syncedTo
	}
}

// value returns mine+theirs (lock-free). Assumes the counter is loaded.
func (a *AtomicCounter) value() int64 {
	switch c := a.data.(type) {
	case *nState:
		return c.mine.Load() + c.theirs.Load()
	case *zState:
		return c.mine.Load() + c.theirs.Load()
	}
	return 0
}

// Get returns mine + last-known others'. Lock-free and DB-free.
func (a *AtomicCounter) Get(ctx context.Context) (int64, error) {
	a.accessed.Store(true)
	if !a.loaded.Load() {
		return 0, ErrCounterNotLoaded
	}
	return a.value(), nil
}

// Increment adds val to mine (Natural rejects val < 0); flushed by the manager on the next cycle. Lock-free and DB-free.
func (a *AtomicCounter) Increment(ctx context.Context, val int64) (int64, error) {
	a.accessed.Store(true)
	if !a.loaded.Load() {
		return 0, ErrCounterNotLoaded
	}
	switch c := a.data.(type) {
	case *nState:
		if val < 0 {
			return 0, ErrDecrementN
		}
		return c.mine.Add(val) + c.theirs.Load(), nil
	case *zState:
		return c.mine.Add(val) + c.theirs.Load(), nil
	}
	return 0, ErrCounterNotLoaded
}
