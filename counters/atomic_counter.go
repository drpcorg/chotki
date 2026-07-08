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
	data     any // *nState | *zState; set once on first successful load
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
		sum, mineDB, rev := rdx.Znative3(tlv, a.db.Source())
		c.theirs.Store(sum - mineDB)
		if first {
			c.mine.Store(mineDB)
			c.lastSynced = mineDB
			c.rev = rev
		} else {
			// Adopt any external change to our own slot so value() stays fresh even without a
			// local increment, and never let our revision fall behind the DB. Unflushed local
			// increments (mine-lastSynced) are preserved: we shift mine by the external delta
			// and re-baseline lastSynced to the observed slot.
			if extDelta := mineDB - c.lastSynced; extDelta != 0 {
				c.mine.Add(extDelta)
			}
			c.lastSynced = mineDB
			c.rev = max(c.rev, rev)
		}
	default:
		return ErrNotCounter
	}
	a.loaded.Store(true) // publish baseline before any lock-free op
	return nil
}

// pendingFlush returns the field-edit op for a counter with unflushed local increments, plus
// an onCommit callback the manager runs iff the batch commits. changed=false means nothing to
// flush. Mutex-free: caller holds the manager mutex, so only lock-free Increment runs alongside.
//
// A Z flush is a read-modify-write: it reads the current DB value of its own src slot, adds the
// delta accumulated since the last flush, and stamps a revision above any already present. This
// keeps a second writer to the same (src, field) slot (e.g. an ORM Zdelta "set") from being
// clobbered and stops the manager's own write from being silently dropped by the merge. Because
// the write is a delta over the observed slot rather than the cached absolute mine, onCommit also
// folds the observed external change into mine so value() stays correct.
func (a *AtomicCounter) pendingFlush() (changed bool, rdt byte, op []byte, onCommit func()) {
	switch c := a.data.(type) {
	case *nState:
		m := c.mine.Load()
		if m == c.lastSynced {
			return false, 0, nil, nil
		}
		return true, rdx.Natural, rdx.Ntlvt(uint64(m), a.db.Source()), func() {
			c.lastSynced = m
		}
	case *zState:
		m := c.mine.Load()
		delta := m - c.lastSynced
		if delta == 0 {
			return false, 0, nil, nil
		}
		_, tlv, err := a.db.ObjectFieldTLV(a.rid.ToOff(a.offset))
		if err != nil {
			return false, 0, nil, nil // can't read the slot; retry next cycle
		}
		_, mineDB, dbRev := rdx.Znative3(tlv, a.db.Source())
		newRev := max(c.rev, dbRev) + 1
		newSlot := mineDB + delta
		extDelta := mineDB - c.lastSynced // net effect of any other writer to our slot
		return true, rdx.ZCounter, rdx.Ztlvt(newSlot, a.db.Source(), newRev), func() {
			c.rev = newRev
			if extDelta != 0 {
				c.mine.Add(extDelta) // fold the external change into our running total
			}
			c.lastSynced = newSlot
		}
	}
	return false, 0, nil, nil
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
