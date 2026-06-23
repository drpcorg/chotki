// Provides AtomicCounter, a lock-free CRDT counter (Natural increment-only or ZCounter
// two-way). Increment/Get touch only in-memory atomics; the owning AtomicCounterManager
// runs a background goroutine that periodically flushes the local contribution (mine) and
// reloads other replicas' contributions (theirs) on a single cadence.
//
// Local increments are visible immediately; other replicas' increments become visible after
// the next reload. Increments since the last flush are lost on a hard crash (a graceful
// Close flushes); this is the deliberate tradeoff for a lock-free hot path.
package counters

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

var ErrNotCounter error = fmt.Errorf("not a counter")
var ErrCounterNotLoaded error = fmt.Errorf("counter not loaded")
var ErrDecrementN error = fmt.Errorf("decrementing natural counter")

// AtomicCounter is the in-memory state for one (rid, offset) field. Increment/Get are
// lock-free; load/flush are mutex-free and assume the caller (the manager) holds its mutex.
type AtomicCounter struct {
	data     any    // *nState | *zState; set once on first successful load
	rid      rdx.ID
	offset   uint64
	db       host.Host
	loaded   atomic.Bool
	accessed atomic.Bool // set by Get/Increment; tells the background tick to reload
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

// load reads the field from the DB and refreshes theirs. On the first successful load it also
// fixes the counter kind (N/Z) and the mine/lastSynced baseline. Mutex-free: the caller (the
// manager cycle or factory) must hold the manager mutex.
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
	a.loaded.Store(true) // publish baseline before any lock-free op is accepted
	return nil
}

// flush persists the full local contribution if it changed since the last flush, reusing the
// existing CommitPacket. lastSynced advances only after a successful commit. Mutex-free: the
// caller must hold the manager mutex.
func (a *AtomicCounter) flush(ctx context.Context) error {
	var rdt byte
	var op []byte
	var synced int64
	switch c := a.data.(type) {
	case *nState:
		m := c.mine.Load()
		if m == c.lastSynced {
			return nil
		}
		rdt, op, synced = rdx.Natural, rdx.Ntlvt(uint64(m), a.db.Source()), m
	case *zState:
		m := c.mine.Load()
		if m == c.lastSynced {
			return nil
		}
		c.rev++
		rdt, op, synced = rdx.ZCounter, rdx.Ztlvt(m, a.db.Source(), c.rev), m
	default:
		return nil // not loaded yet: nothing to flush
	}
	body := protocol.Records{
		protocol.Record('F', rdx.ZipUint64(a.offset)),
		protocol.Record(rdt, op),
	}
	if _, err := a.db.CommitPacket(ctx, 'E', a.rid.ZeroOff(), body); err != nil {
		return err
	}
	switch c := a.data.(type) {
	case *nState:
		c.lastSynced = synced
	case *zState:
		c.lastSynced = synced
	}
	return nil
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

// Get returns the current value (mine + last-known others'). Lock-free and DB-free.
func (a *AtomicCounter) Get(ctx context.Context) (int64, error) {
	a.accessed.Store(true) // request a reload on the next background tick
	if !a.loaded.Load() {
		return 0, ErrCounterNotLoaded
	}
	return a.value(), nil
}

// Increment adds val to the local contribution (Natural rejects val < 0) and returns the new
// value. Lock-free and DB-free; the change is flushed by the manager on the next cycle.
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
