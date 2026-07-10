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
		sum, mineDB := rdx.Nnative2(tlv, a.db.Source())
		c.theirs.Store(int64(sum) - int64(mineDB))
		if first {
			c.mine.Store(int64(mineDB))
			c.lastSynced = int64(mineDB)
		} else {
			// adopt an external change to our slot (e.g. ORM AddToNField):
			// shift mine by the delta and re-baseline, preserving unflushed
			// increments so value() stays fresh and nothing is lost to the merge.
			if extDelta := int64(mineDB) - c.lastSynced; extDelta != 0 {
				c.mine.Add(extDelta)
			}
			c.lastSynced = int64(mineDB)
		}
	case *zState:
		sum, mineDB, rev := rdx.Znative3(tlv, a.db.Source())
		c.theirs.Store(sum - mineDB)
		if first {
			c.mine.Store(mineDB)
			c.lastSynced = mineDB
			c.rev = rev
		} else {
			// as nState above; also never let our revision fall behind the DB.
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

// pendingFlush returns the edit op for a counter with unflushed increments plus
// an onCommit run iff the batch commits (changed=false: nothing to flush).
// Caller holds the manager mutex.
//
// The flush is a read-modify-write: it commits slot + delta (not the cached
// mine), for Z stamps a revision above any present, and onCommit folds the
// observed external change into mine — so a concurrent writer isn't clobbered.
func (a *AtomicCounter) pendingFlush() (changed bool, rdt byte, op []byte, onCommit func()) {
	switch c := a.data.(type) {
	case *nState:
		m := c.mine.Load()
		delta := m - c.lastSynced
		if delta == 0 {
			return false, 0, nil, nil
		}
		// N is grow-only, so newSlot >= mineDB and the merge always accepts it;
		// no revision needed (unlike Z).
		_, tlv, err := a.db.ObjectFieldTLV(a.rid.ToOff(a.offset))
		if err != nil {
			// log so the skip isn't mistaken for a flush; delta is kept
			a.db.Logger().Warn("counter flush: cannot read slot, retrying next cycle",
				"rid", a.rid.String(), "offset", a.offset, "err", err)
			return false, 0, nil, nil
		}
		_, mineDB := rdx.Nnative2(tlv, a.db.Source())
		newSlot := int64(mineDB) + delta
		extDelta := int64(mineDB) - c.lastSynced // net effect of any other writer to our slot
		return true, rdx.Natural, rdx.Ntlvt(uint64(newSlot), a.db.Source()), func() {
			if extDelta != 0 {
				c.mine.Add(extDelta) // fold the external change into our running total
			}
			c.lastSynced = newSlot
		}
	case *zState:
		m := c.mine.Load()
		delta := m - c.lastSynced
		if delta == 0 {
			return false, 0, nil, nil
		}
		_, tlv, err := a.db.ObjectFieldTLV(a.rid.ToOff(a.offset))
		if err != nil {
			// log so the skip isn't mistaken for a flush; delta is kept
			a.db.Logger().Warn("counter flush: cannot read slot, retrying next cycle",
				"rid", a.rid.String(), "offset", a.offset, "err", err)
			return false, 0, nil, nil
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
