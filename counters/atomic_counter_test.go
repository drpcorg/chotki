package counters_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/counters"
	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	testutils "github.com/drpcorg/chotki/test_utils"
	"github.com/stretchr/testify/assert"
)

// openReplica opens a fresh chotki replica. A long period keeps the background goroutine
// from firing during tests, so cycles can be driven deterministically via SyncCounters.
func openReplica(t *testing.T, src uint64, period time.Duration) *chotki.Chotki {
	t.Helper()
	dir, err := os.MkdirTemp("", "*")
	assert.NoError(t, err)
	cho, err := chotki.Open(dir, chotki.Options{
		Src:               src,
		Name:              "replica",
		Options:           pebble.Options{ErrorIfExists: true},
		CounterSyncPeriod: period,
	})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = cho.Close() })
	return cho
}

// newCounterObject creates a class with an N field (offset 1) and a Z field (offset 2)
// and an object of that class, returning the object id.
func newCounterObject(t *testing.T, cho *chotki.Chotki) rdx.ID {
	t.Helper()
	cid, err := cho.NewClass(context.Background(), rdx.ID0,
		classes.Field{Name: "n", RdxType: rdx.Natural},
		classes.Field{Name: "z", RdxType: rdx.ZCounter},
	)
	assert.NoError(t, err)
	rid, err := cho.NewObjectTLV(context.Background(), cid,
		protocol.Records{
			protocol.Record('N', rdx.Ntlv(0)),
			protocol.Record('Z', rdx.Ztlv(0)),
		})
	assert.NoError(t, err)
	return rid
}

// persistedN reads the Natural field's persisted total straight from the DB, bypassing any
// in-memory counter state, so it actually verifies that a flush happened.
func persistedN(t *testing.T, cho *chotki.Chotki, rid rdx.ID, offset uint64) int64 {
	t.Helper()
	rdt, tlv, err := cho.ObjectFieldTLV(rid.ToOff(offset))
	assert.NoError(t, err)
	assert.EqualValues(t, rdx.Natural, rdt)
	sum, _ := rdx.Nnative2(tlv, cho.Source())
	return int64(sum)
}

// persistedZ is persistedN for a ZCounter field.
func persistedZ(t *testing.T, cho *chotki.Chotki, rid rdx.ID, offset uint64) int64 {
	t.Helper()
	rdt, tlv, err := cho.ObjectFieldTLV(rid.ToOff(offset))
	assert.NoError(t, err)
	assert.EqualValues(t, rdx.ZCounter, rdt)
	sum, _, _ := rdx.Znative3(tlv, cho.Source())
	return sum
}

func TestAtomicCounter(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	rid := newCounterObject(t, a)

	// Two handles for the same field share one state and stay consistent.
	ca := a.Counter(rid, 1)
	cb := a.Counter(rid, 1)

	res, err := ca.Increment(ctx, 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, res)

	res, err = cb.Increment(ctx, 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, res)

	res, err = ca.Increment(ctx, 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, res)
}

func TestBatchedNoLossNatural(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	rid := newCounterObject(t, a)

	c := a.Counter(rid, 1) // batched, lock-free
	for i := 0; i < 5; i++ {
		_, err := c.Increment(ctx, 1)
		assert.NoError(t, err)
	}

	// Local increments are visible immediately, before any flush.
	got, err := c.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, got)
	// ...but nothing is in the DB yet (no cycle ran).
	assert.EqualValues(t, 0, persistedN(t, a, rid, 1))

	// One cycle flushes; a fresh DB read (not the in-memory mine) confirms persistence.
	a.SyncCounters(ctx)
	assert.EqualValues(t, 5, persistedN(t, a, rid, 1))

	got, err = c.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, got)
}

func TestBatchedZCounterTwoWay(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	rid := newCounterObject(t, a)

	cz := a.Counter(rid, 2) // Z field
	_, err := cz.Increment(ctx, 10)
	assert.NoError(t, err)
	_, err = cz.Increment(ctx, -3)
	assert.NoError(t, err)

	got, err := cz.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 7, got)
	assert.EqualValues(t, 0, persistedZ(t, a, rid, 2)) // not flushed yet

	a.SyncCounters(ctx)
	assert.EqualValues(t, 7, persistedZ(t, a, rid, 2)) // fresh DB read

	got, err = cz.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 7, got)
}

func TestNaturalRejectsDecrement(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	rid := newCounterObject(t, a)

	_, err := a.Counter(rid, 1).Increment(ctx, -1)
	assert.ErrorIs(t, err, counters.ErrDecrementN)
}

func TestNotLoadedThenRetry(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	b := openReplica(t, 0x1b, time.Hour)
	rid := newCounterObject(t, a)

	// b does not have the object yet -> initial load fails -> not loaded.
	cb := b.Counter(rid, 1)
	_, err := cb.Get(ctx)
	assert.ErrorIs(t, err, counters.ErrCounterNotLoaded)

	// Give a a value, propagate to b, then a cycle on b retries the load.
	ca := a.Counter(rid, 1)
	_, err = ca.Increment(ctx, 4)
	assert.NoError(t, err)
	a.SyncCounters(ctx)
	testutils.SyncData(a, b) // returns io.EOF on normal completion; discard like the rest of the codebase
	b.SyncCounters(ctx)

	got, err := cb.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, got)
}

func TestConcurrentIncrementsNoLoss(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	rid := newCounterObject(t, a)

	c := a.Counter(rid, 1) // loaded at creation (object exists)

	const goroutines, perG = 8, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, err := c.Increment(ctx, 1); err != nil {
					t.Errorf("increment: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	got, err := c.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, goroutines*perG, got)

	a.SyncCounters(ctx)
	assert.EqualValues(t, goroutines*perG, persistedN(t, a, rid, 1))
}

func TestEventualConsistencyTwoReplicas(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	b := openReplica(t, 0x1b, time.Hour)
	rid := newCounterObject(t, a)
	testutils.SyncData(a, b) // b gets the class + object

	ca := a.Counter(rid, 1)
	cb := b.Counter(rid, 1)

	_, err := ca.Increment(ctx, 3)
	assert.NoError(t, err)
	_, err = cb.Increment(ctx, 5)
	assert.NoError(t, err)

	// Bounded staleness: before any sync, each replica sees only its own contribution.
	gotA0, err := ca.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, gotA0)
	gotB0, err := cb.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, gotB0)

	// Flush both, exchange, reload both.
	a.SyncCounters(ctx)
	b.SyncCounters(ctx)
	testutils.SyncData(a, b)
	a.SyncCounters(ctx)
	b.SyncCounters(ctx)

	gotA, err := ca.Get(ctx)
	assert.NoError(t, err)
	gotB, err := cb.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 8, gotA)
	assert.EqualValues(t, 8, gotB)
}

// Headline guarantee: absent a crash, no event is missed. Every increment on A eventually
// shows up on B, exactly.
func TestNoMissedEventsOverManyCycles(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	b := openReplica(t, 0x1b, time.Hour)
	rid := newCounterObject(t, a)
	testutils.SyncData(a, b)

	ca := a.Counter(rid, 1)
	cb := b.Counter(rid, 1)

	total := int64(0)
	for round := 1; round <= 10; round++ {
		for i := 0; i < round; i++ { // varying burst sizes
			_, err := ca.Increment(ctx, 1)
			assert.NoError(t, err)
			total++
		}
		a.SyncCounters(ctx)
		testutils.SyncData(a, b)
		b.SyncCounters(ctx)
	}

	got, err := cb.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, total, got, "b must see every a increment")
}

func TestManyCountersOneCycle(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)

	cid, err := a.NewClass(ctx, rdx.ID0, classes.Field{Name: "n", RdxType: rdx.Natural})
	assert.NoError(t, err)

	const n = 50
	rids := make([]rdx.ID, n)
	for i := 0; i < n; i++ {
		rid, err := a.NewObjectTLV(ctx, cid, protocol.Records{protocol.Record('N', rdx.Ntlv(0))})
		assert.NoError(t, err)
		rids[i] = rid
		_, err = a.Counter(rid, 1).Increment(ctx, int64(i+1))
		assert.NoError(t, err)
	}

	a.SyncCounters(ctx) // one cycle flushes + reloads all of them

	for i := 0; i < n; i++ {
		assert.EqualValues(t, i+1, persistedN(t, a, rids[i], 1))
	}
}

func TestGracefulShutdownFlushes(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "*")
	assert.NoError(t, err)

	a, err := chotki.Open(dir, chotki.Options{
		Src: 0x1a, Name: "replica",
		Options:           pebble.Options{ErrorIfExists: true},
		CounterSyncPeriod: time.Hour, // no background cycle; rely on shutdown flush
	})
	assert.NoError(t, err)
	rid := newCounterObject(t, a)

	c := a.Counter(rid, 1)
	_, err = c.Increment(ctx, 5) // lock-free; NOT yet flushed (no cycle ran)
	assert.NoError(t, err)
	assert.NoError(t, a.Close()) // graceful shutdown must flush the 5

	// Reopen the same directory (it now exists).
	a2, err := chotki.Open(dir, chotki.Options{
		Src: 0x1a, Name: "replica",
		Options:           pebble.Options{ErrorIfExists: false},
		CounterSyncPeriod: time.Hour,
	})
	assert.NoError(t, err)
	defer a2.Close()

	assert.EqualValues(t, 5, persistedN(t, a2, rid, 1))
	got, err := a2.Counter(rid, 1).Get(ctx) // fresh handle, mine loaded from DB
	assert.NoError(t, err)
	assert.EqualValues(t, 5, got)
}

// Z counters carry a per-source revision that must be restored from the DB on reopen, or a
// post-reopen flush could emit a stale revision and be dropped by the merge (silent loss).
func TestZCounterRevisionAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "*")
	assert.NoError(t, err)

	a, err := chotki.Open(dir, chotki.Options{
		Src: 0x1a, Name: "replica",
		Options:           pebble.Options{ErrorIfExists: true},
		CounterSyncPeriod: time.Hour,
	})
	assert.NoError(t, err)
	rid := newCounterObject(t, a)
	_, err = a.Counter(rid, 2).Increment(ctx, 7)
	assert.NoError(t, err)
	a.SyncCounters(ctx) // flush Z at rev 1
	assert.NoError(t, a.Close())

	a2, err := chotki.Open(dir, chotki.Options{
		Src: 0x1a, Name: "replica",
		Options:           pebble.Options{ErrorIfExists: false},
		CounterSyncPeriod: time.Hour,
	})
	assert.NoError(t, err)
	got, err := a2.Counter(rid, 2).Increment(ctx, 3) // must use rev 2 (> persisted rev 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, got)
	a2.SyncCounters(ctx)
	assert.NoError(t, a2.Close())

	a3, err := chotki.Open(dir, chotki.Options{
		Src: 0x1a, Name: "replica",
		Options:           pebble.Options{ErrorIfExists: false},
		CounterSyncPeriod: time.Hour,
	})
	assert.NoError(t, err)
	defer a3.Close()
	assert.EqualValues(t, 10, persistedZ(t, a3, rid, 2)) // both flush generations survived
	got, err = a3.Counter(rid, 2).Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, got)
}

// Many goroutines creating the same counter must all share one state (so increments add up
// rather than diverging) and be race-free.
func TestConcurrentCounterCreation(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	rid := newCounterObject(t, a)

	const n = 20
	handles := make([]*counters.AtomicCounter, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			handles[i] = a.Counter(rid, 1)
		}(i)
	}
	wg.Wait()

	// All handles share one state: one increment via each totals n, not 1.
	for _, h := range handles {
		_, err := h.Increment(ctx, 1)
		assert.NoError(t, err)
	}
	got, err := handles[0].Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, n, got)

	a.SyncCounters(ctx)
	assert.EqualValues(t, n, persistedN(t, a, rid, 1))
}

// flushFaultHost wraps a host and fails CommitPacket for one object ref when armed.
type flushFaultHost struct {
	host.Host
	failRef rdx.ID
	fail    atomic.Bool
}

func (f *flushFaultHost) CommitPacket(ctx context.Context, lit byte, ref rdx.ID, body protocol.Records) (rdx.ID, error) {
	if f.fail.Load() && ref == f.failRef {
		return rdx.BadId, fmt.Errorf("injected flush failure")
	}
	return f.Host.CommitPacket(ctx, lit, ref, body)
}

// One counter's flush failure must not block the others, must not advance its lastSynced,
// and must be retried (no increment lost) once the fault clears.
func TestFlushFailureIsolation(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)

	cid, err := a.NewClass(ctx, rdx.ID0, classes.Field{Name: "n", RdxType: rdx.Natural})
	assert.NoError(t, err)
	goodRid, err := a.NewObjectTLV(ctx, cid, protocol.Records{protocol.Record('N', rdx.Ntlv(0))})
	assert.NoError(t, err)
	badRid, err := a.NewObjectTLV(ctx, cid, protocol.Records{protocol.Record('N', rdx.Ntlv(0))})
	assert.NoError(t, err)

	fh := &flushFaultHost{Host: a, failRef: badRid.ZeroOff()}
	mgr := counters.NewAtomicCounterManager(fh, 0, nil)
	good := mgr.Counter(goodRid, 1)
	bad := mgr.Counter(badRid, 1)
	_, err = good.Increment(ctx, 3)
	assert.NoError(t, err)
	_, err = bad.Increment(ctx, 7)
	assert.NoError(t, err)

	fh.fail.Store(true)
	mgr.Cycle(ctx) // good flushes; bad's flush fails

	assert.EqualValues(t, 3, persistedN(t, a, goodRid, 1)) // isolated: good still persisted
	assert.EqualValues(t, 0, persistedN(t, a, badRid, 1))  // bad did not persist

	fh.fail.Store(false)
	mgr.Cycle(ctx) // bad retries with its full mine

	assert.EqualValues(t, 7, persistedN(t, a, badRid, 1)) // no increment lost
}

// The production background ticker (not just the manual SyncCounters path) must flush.
func TestBackgroundTimerFlushes(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, 20*time.Millisecond) // real, short period

	rid := newCounterObject(t, a)
	_, err := a.Counter(rid, 1).Increment(ctx, 9) // batched; NO manual SyncCounters
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		rdt, tlv, err := a.ObjectFieldTLV(rid.ToOff(1))
		if err != nil || rdt != rdx.Natural {
			return false
		}
		sum, _ := rdx.Nnative2(tlv, a.Source())
		return sum == 9
	}, 2*time.Second, 10*time.Millisecond)
}

// A field whose baseline load fails (object not yet replicated) must keep being retried by
// the background ticker until the data arrives — even if it is never re-accessed after the
// retry signal is consumed.
func TestNotLoadedRetriedByBackgroundTick(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	b := openReplica(t, 0x1b, 20*time.Millisecond) // real, short period
	rid := newCounterObject(t, a)

	_, err := a.Counter(rid, 1).Increment(ctx, 6)
	assert.NoError(t, err)
	a.SyncCounters(ctx)

	cb := b.Counter(rid, 1) // initial load fails: b lacks the object
	_, err = cb.Get(ctx)    // arms the accessed retry signal
	assert.ErrorIs(t, err, counters.ErrCounterNotLoaded)

	// Let a tick consume the accessed signal while the data is still absent (load fails),
	// so recovery can only come from retrying an unloaded field — not from the stale signal.
	time.Sleep(100 * time.Millisecond)
	testutils.SyncData(a, b) // b now has the object

	// Wait on the ticker alone; do NOT call Get during the wait (a Get would re-arm the
	// retry and mask a regression). One settled Get afterwards must see the value.
	time.Sleep(400 * time.Millisecond)
	got, err := cb.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 6, got)
}
