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

// openReplica opens a fresh replica; a long period prevents background cycles so tests drive them manually.
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

// newCounterObject creates a class with an N field (offset 1) and a Z field (offset 2), then returns the object id.
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

// persistedN reads the Natural field's total directly from the DB, bypassing in-memory state.
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

	// Two handles for the same field share state.
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

	// Local increments are visible before flush; DB is 0 until a cycle runs.
	got, err := c.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, got)
	assert.EqualValues(t, 0, persistedN(t, a, rid, 1))

	// One cycle flushes; fresh DB read confirms persistence.
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
	assert.EqualValues(t, 0, persistedZ(t, a, rid, 2)) // unflushed

	a.SyncCounters(ctx)
	assert.EqualValues(t, 7, persistedZ(t, a, rid, 2))

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

	// b lacks the object -> initial load fails.
	cb := b.Counter(rid, 1)
	_, err := cb.Get(ctx)
	assert.ErrorIs(t, err, counters.ErrCounterNotLoaded)

	// Propagate a value to b; b's next cycle retries the load.
	ca := a.Counter(rid, 1)
	_, err = ca.Increment(ctx, 4)
	assert.NoError(t, err)
	a.SyncCounters(ctx)
	testutils.SyncData(a, b) // returns io.EOF on normal completion
	b.SyncCounters(ctx)

	got, err := cb.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, got)
}

// A counter whose object wasn't local at first touch must load on the next Counter()
// lookup once the object is readable, WITHOUT waiting for a background cycle. Regression:
// the manager loaded once at handle creation and cached the unloaded handle, so a
// just-synced counter stayed ErrCounterNotLoaded until the next tick. Callers (e.g. dproxy)
// re-resolve via Counter() on every op, so Counter() must return a loaded handle.
func TestLazyLoadOnAccessAfterSync(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	b := openReplica(t, 0x1b, time.Hour)
	rid := newCounterObject(t, a)

	// b touches the counter before it has the object -> handle cached unloaded.
	_, err := b.Counter(rid, 1).Get(ctx)
	assert.ErrorIs(t, err, counters.ErrCounterNotLoaded)

	// a contributes 4; replicate the object+value to b. Crucially, do NOT run b's
	// counter cycle -- recovery must come from Counter() reloading on the next lookup.
	ca := a.Counter(rid, 1)
	_, err = ca.Increment(ctx, 4)
	assert.NoError(t, err)
	a.SyncCounters(ctx)
	testutils.SyncData(a, b)

	// A fresh Counter() lookup (as callers do per op) must return a loaded handle.
	got, err := b.Counter(rid, 1).Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, got)

	// Increment via a fresh lookup also works and adds to the loaded value.
	got, err = b.Counter(rid, 1).Increment(ctx, 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, got)
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

	// Before sync, each replica sees only its own contribution.
	gotA0, err := ca.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, gotA0)
	gotB0, err := cb.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, gotB0)

	// Flush, exchange, reload both sides.
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

// Headline: no missed events — every increment on A must appear on B exactly.
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

	a.SyncCounters(ctx) // one cycle flushes all 50

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
		CounterSyncPeriod: time.Hour, // no background cycle; flush happens on Close
	})
	assert.NoError(t, err)
	rid := newCounterObject(t, a)

	c := a.Counter(rid, 1)
	_, err = c.Increment(ctx, 5) // not yet flushed
	assert.NoError(t, err)
	assert.NoError(t, a.Close()) // shutdown must flush the 5

	// Reopen the same directory (it now exists).
	a2, err := chotki.Open(dir, chotki.Options{
		Src: 0x1a, Name: "replica",
		Options:           pebble.Options{ErrorIfExists: false},
		CounterSyncPeriod: time.Hour,
	})
	assert.NoError(t, err)
	defer a2.Close()

	assert.EqualValues(t, 5, persistedN(t, a2, rid, 1))
	got, err := a2.Counter(rid, 1).Get(ctx) // fresh handle loaded from DB
	assert.NoError(t, err)
	assert.EqualValues(t, 5, got)
}

// Z counters carry a per-source revision; it must survive reopen or a post-reopen flush emits a stale rev and is silently dropped.
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
	a.SyncCounters(ctx) // persists Z at rev 1
	assert.NoError(t, a.Close())

	a2, err := chotki.Open(dir, chotki.Options{
		Src: 0x1a, Name: "replica",
		Options:           pebble.Options{ErrorIfExists: false},
		CounterSyncPeriod: time.Hour,
	})
	assert.NoError(t, err)
	got, err := a2.Counter(rid, 2).Increment(ctx, 3) // must use rev 2 > persisted rev 1
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
	assert.EqualValues(t, 10, persistedZ(t, a3, rid, 2)) // both flush generations persisted
	got, err = a3.Counter(rid, 2).Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, got)
}

// Concurrent Counter() calls for the same field must all share one state and be race-free.
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

	// All handles share state: n increments of 1 must total n.
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

// batchFaultHost wraps a host and fails the whole CommitBatch when armed.
type batchFaultHost struct {
	host.Host
	fail atomic.Bool
}

func (f *batchFaultHost) CommitBatch(ctx context.Context, edits []host.Edit) error {
	if f.fail.Load() {
		return fmt.Errorf("injected batch flush failure")
	}
	return f.Host.CommitBatch(ctx, edits)
}

// Batch commit failure is all-or-nothing: nothing persisted, nothing marked; retained increments flush on the next cycle.
func TestBatchFlushFailureRetried(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)

	cid, err := a.NewClass(ctx, rdx.ID0, classes.Field{Name: "n", RdxType: rdx.Natural})
	assert.NoError(t, err)
	rid1, err := a.NewObjectTLV(ctx, cid, protocol.Records{protocol.Record('N', rdx.Ntlv(0))})
	assert.NoError(t, err)
	rid2, err := a.NewObjectTLV(ctx, cid, protocol.Records{protocol.Record('N', rdx.Ntlv(0))})
	assert.NoError(t, err)

	fh := &batchFaultHost{Host: a}
	mgr := counters.NewAtomicCounterManager(fh, 0, nil)
	_, err = mgr.Counter(rid1, 1).Increment(ctx, 3)
	assert.NoError(t, err)
	_, err = mgr.Counter(rid2, 1).Increment(ctx, 7)
	assert.NoError(t, err)

	fh.fail.Store(true)
	mgr.Cycle(ctx) // batch fails: nothing persisted
	assert.EqualValues(t, 0, persistedN(t, a, rid1, 1))
	assert.EqualValues(t, 0, persistedN(t, a, rid2, 1))

	fh.fail.Store(false)
	mgr.Cycle(ctx) // retry flushes retained increments
	assert.EqualValues(t, 3, persistedN(t, a, rid1, 1))
	assert.EqualValues(t, 7, persistedN(t, a, rid2, 1))
}

// The background ticker (not just manual SyncCounters) must flush counters.
func TestBackgroundTimerFlushes(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, 20*time.Millisecond) // real, short period

	rid := newCounterObject(t, a)
	_, err := a.Counter(rid, 1).Increment(ctx, 9) // no manual SyncCounters
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

// A failed baseline load must keep being retried by the ticker until data arrives, even if never re-accessed.
func TestNotLoadedRetriedByBackgroundTick(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	b := openReplica(t, 0x1b, 20*time.Millisecond) // real, short period
	rid := newCounterObject(t, a)

	_, err := a.Counter(rid, 1).Increment(ctx, 6)
	assert.NoError(t, err)
	a.SyncCounters(ctx)

	cb := b.Counter(rid, 1) // load fails: b lacks the object
	_, err = cb.Get(ctx)    // arms the accessed-retry signal
	assert.ErrorIs(t, err, counters.ErrCounterNotLoaded)

	// Let a tick consume the accessed signal while data is still absent, so recovery
	// can only come from retrying unloaded fields — not the stale signal.
	time.Sleep(100 * time.Millisecond)
	testutils.SyncData(a, b) // b now has the object

	// Wait for ticker alone; no Get during wait (would re-arm retry, masking regressions).
	time.Sleep(400 * time.Millisecond)
	got, err := cb.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 6, got)
}

// Increments from many goroutines concurrent with flush cycles; must be race-free and lose no increments.
func TestConcurrentIncrementDuringCycles(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour) // manual cycles maximize contention
	rid := newCounterObject(t, a)
	c := a.Counter(rid, 1)

	const goroutines, perG = 8, 200
	stop := make(chan struct{})
	var cyc sync.WaitGroup
	cyc.Add(1)
	go func() {
		defer cyc.Done()
		for {
			select {
			case <-stop:
				return
			default:
				a.SyncCounters(ctx) // flush+reload concurrent with increments
			}
		}
	}()

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
	close(stop)
	cyc.Wait()

	a.SyncCounters(ctx) // final flush
	assert.EqualValues(t, goroutines*perG, persistedN(t, a, rid, 1))
	got, err := c.Get(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, goroutines*perG, got)
}
