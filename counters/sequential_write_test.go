package counters_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A Z slot has two local read-modify-write writers: the counter manager's
// flush and "set"-style flows (an ORM Zdelta computed from a loaded value).
// Both must hold the host's sequential-write bracket across their read AND
// commit; without it one lands between the other's read and commit, the two
// ops collide on the revision, and the merge silently drops one (a reset is
// ignored, or flushed usage is erased).
//
// The set goroutine below is the reference pattern for such flows (e.g. the
// keymanager balance/renewal resets): StartSequentialWrite → read the CURRENT
// value → compute Zdelta from it → commit → EndSequentialWrite. Reading
// before the bracket (or from an older snapshot) fixes nothing.
//
// With every RMW bracketed, mixed concurrent adders must converge EXACTLY:
// final = increments + 1000 per set-writer round. Any lost update makes the
// total come up short, so this is a deterministic guard for the protocol.
func TestZSequentialWritersConvergeExactly(t *testing.T) {
	ctx := context.Background()
	a := openReplica(t, 0x1a, time.Hour)
	rid := newCounterObject(t, a)
	fid := rid.ToOff(2) // Z field

	c := a.Counter(rid, 2)
	_, err := c.Increment(ctx, 0)
	require.NoError(t, err)

	const incs = 200 // lock-free +1 increments, flushed periodically
	const sets = 20  // bracketed read-modify-write adds of +1000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() { // manager path: increments + periodic flushes
		defer wg.Done()
		for i := 0; i < incs; i++ {
			_, e := c.Increment(ctx, 1)
			assert.NoError(t, e)
			if i%20 == 0 {
				a.SyncCounters(ctx)
			}
		}
	}()

	go func() { // set path: read-modify-write inside the bracket
		defer wg.Done()
		for i := 0; i < sets; i++ {
			a.StartSequentialWrite()
			_, tlv := a.GetFieldTLV(fid)
			total := rdx.Znative(tlv)
			_, e := a.EditFieldTLV(ctx, fid,
				protocol.Record(rdx.ZCounter, rdx.Zdelta(tlv, total+1000, a.Clock())))
			a.EndSequentialWrite()
			assert.NoError(t, e)
		}
	}()

	wg.Wait()
	a.SyncCounters(ctx) // final flush + reload

	want := int64(incs + 1000*sets)
	assert.EqualValues(t, want, persistedZ(t, a, rid, 2),
		"a lost read-modify-write update: some increment or set was dropped")
	got, err := c.Get(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, want, got)
}
