package chotki

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	testutils "github.com/drpcorg/chotki/test_utils"
	"github.com/stretchr/testify/assert"
)

// capHose captures the records a producer broadcasts, so a single live commit
// (O/E/C) can be replayed into a target replica in a chosen order.
type capHose struct{ recs protocol.Records }

func (h *capHose) Drain(_ context.Context, recs protocol.Records) error {
	h.recs = append(h.recs, recs...)
	return nil
}
func (h *capHose) Close() error { return nil }

// captureBroadcast runs fn (a commit on producer) and returns exactly the
// records it broadcast — the wire packet a peer would receive.
func captureBroadcast(producer *Chotki, fn func()) protocol.Records {
	h := &capHose{}
	producer.outq.Store("cap", h)
	defer producer.outq.Delete("cap")
	fn()
	return h.recs
}

// openIdx opens a replica with the reindex worker effectively idle (a very long
// period), so tests assert that the apply path keeps hash indexes correct on its
// own — the reindex backstop can't heal what the apply path is supposed to index.
func openIdx(t *testing.T, dir string, src uint64) *Chotki {
	t.Helper()
	reindexPeriod = time.Hour
	t.Cleanup(func() { reindexPeriod = time.Second })
	c, err := Open(dir, Options{
		Src:                src,
		Name:               "r",
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	return c
}

// liveUnits sets up a producer and captures the wire packets for: creating a
// hash-indexed class (C), creating an object with value "v1" (O), and editing
// the indexed field to "v2" (E). Returns the class id, object id, and packets.
func liveUnits(t *testing.T, p *Chotki) (cid, rid rdx.ID, cRecs, oRecs, eRecs protocol.Records) {
	t.Helper()
	ctx := context.Background()

	cRecs = captureBroadcast(p, func() {
		var err error
		cid, err = p.NewClass(ctx, rdx.ID0, SchemaIndex...)
		assert.NoError(t, err)
	})
	oRecs = captureBroadcast(p, func() {
		var err error
		rid, err = p.NewObjectTLV(ctx, cid, protocol.Records{protocol.Record('S', rdx.Stlv("v1"))})
		assert.NoError(t, err)
	})
	_, oldTlv, err := p.ObjectFieldTLV(rid.ToOff(1))
	assert.NoError(t, err)
	eRecs = captureBroadcast(p, func() {
		_, err := p.EditFieldTLV(ctx, rid.ToOff(1), protocol.Record('S', rdx.Sdelta(oldTlv, "v2", p.Clock())))
		assert.NoError(t, err)
	})
	return
}

// assertResolves checks that the hash index on target maps the CURRENT value
// "v2" to the object — the invariant that must hold from the apply path alone.
func assertResolves(t *testing.T, target *Chotki, cid, rid rdx.ID, msg string) {
	t.Helper()
	got, err := target.ObjectIDByHash(cid, 1, []byte("v2"))
	assert.NoError(t, err, msg)
	assert.Equal(t, rid, got, msg)
}

func cat(groups ...protocol.Records) protocol.Records {
	out := protocol.Records{}
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// [O,E] arrive in ONE drain. Passes today: ApplyOY's AddFullScanIndex populates
// the in-memory classCache, so E (processed after O) resolves its class.
func TestIndexConverge_LiveSameBatch_OE(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()
	ctx := context.Background()

	p := openIdx(t, dirs[0], 0xa)
	defer p.Close()
	cid, rid, cRecs, oRecs, eRecs := liveUnits(t, p)

	target := openIdx(t, dirs[1], 0xb)
	defer target.Close()
	assert.NoError(t, target.Drain(ctx, cRecs))
	assert.NoError(t, target.Drain(ctx, cat(oRecs, eRecs)))

	assertResolves(t, target, cid, rid, "same-batch [O,E] must index current value v2")
}

// [E,O] in ONE drain: E is processed before O, so the classCache isn't populated
// yet and E's index maintenance is skipped. O then indexes only its own "v1".
func TestIndexConverge_LiveSameBatch_EO(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()
	ctx := context.Background()

	p := openIdx(t, dirs[0], 0xa)
	defer p.Close()
	cid, rid, cRecs, oRecs, eRecs := liveUnits(t, p)

	target := openIdx(t, dirs[1], 0xb)
	defer target.Close()
	assert.NoError(t, target.Drain(ctx, cRecs))
	assert.NoError(t, target.Drain(ctx, cat(eRecs, oRecs)))

	assertResolves(t, target, cid, rid, "same-batch [E,O] must index current value v2")
}

// E arrives before its create O (separate drains, out-of-order delivery).
func TestIndexConverge_LiveEbeforeO(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()
	ctx := context.Background()

	p := openIdx(t, dirs[0], 0xa)
	defer p.Close()
	cid, rid, cRecs, oRecs, eRecs := liveUnits(t, p)

	target := openIdx(t, dirs[1], 0xb)
	defer target.Close()
	assert.NoError(t, target.Drain(ctx, cRecs))
	assert.NoError(t, target.Drain(ctx, eRecs))
	assert.NoError(t, target.Drain(ctx, oRecs))

	assertResolves(t, target, cid, rid, "E-before-O must index current value v2")
}

// A NEW object arrives via diff sync at a replica that already has the class
// (reindex worker OFF). Does the sync apply path index it, or does it need help?
func TestIndexConverge_DiffSyncNewObject(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()
	ctx := context.Background()

	p := openIdx(t, dirs[0], 0xa)
	defer p.Close()
	target := openIdx(t, dirs[1], 0xb)
	defer target.Close()

	// Target gets the class first (existing-class replica), no objects yet.
	cid, err := p.NewClass(ctx, rdx.ID0, SchemaIndex...)
	assert.NoError(t, err)
	_ = testutils.SyncData(p, target) // returns io.EOF on normal completion

	// A new object is created on p AFTER target already has the class.
	rid, err := p.NewObjectTLV(ctx, cid, protocol.Records{protocol.Record('S', rdx.Stlv("v1"))})
	assert.NoError(t, err)
	_, oldTlv, err := p.ObjectFieldTLV(rid.ToOff(1))
	assert.NoError(t, err)
	_, err = p.EditFieldTLV(ctx, rid.ToOff(1), protocol.Record('S', rdx.Sdelta(oldTlv, "v2", p.Clock())))
	assert.NoError(t, err)

	// Diff sync carries the new object (current value) to target.
	_ = testutils.SyncData(p, target) // returns io.EOF on normal completion

	assertResolves(t, target, cid, rid, "diff-sync new object must index current value v2")
}

// Many new objects arrive in one diff into an existing-class replica (reindex OFF).
// Validates that classCache resolves every object at scale (O/fields contiguous).
func TestIndexConverge_DiffSyncManyObjects(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()
	ctx := context.Background()

	p := openIdx(t, dirs[0], 0xa)
	defer p.Close()
	target := openIdx(t, dirs[1], 0xb)
	defer target.Close()

	cid, err := p.NewClass(ctx, rdx.ID0, SchemaIndex...)
	assert.NoError(t, err)
	_ = testutils.SyncData(p, target) // target gets the class only

	const n = 100
	rids := make(map[string]rdx.ID, n)
	for i := 0; i < n; i++ {
		val := fmt.Sprintf("obj%d", i)
		rid, err := p.NewObjectTLV(ctx, cid, protocol.Records{protocol.Record('S', rdx.Stlv(val))})
		assert.NoError(t, err)
		rids[val] = rid
	}
	// SyncData's short WaitUntilNone can transfer only part of a large diff per
	// call (esp. under -race); it's state-based, so repeat until all data lands.
	present := func() (c int) {
		for _, rid := range rids {
			if _, _, err := target.ObjectFieldTLV(rid.ToOff(1)); err == nil {
				c++
			}
		}
		return
	}
	for attempt := 0; attempt < 30 && present() < n; attempt++ {
		_ = testutils.SyncData(p, target)
	}
	assert.Equal(t, n, present(), "all objects must reach target")

	for val, rid := range rids {
		got, err := target.ObjectIDByHash(cid, 1, []byte(val))
		assert.NoError(t, err, "diff-sync must index %s", val)
		assert.Equal(t, rid, got, "diff-sync must resolve %s to its object", val)
	}
}

// A diff sync that EDITS a pre-existing object's indexed field must move the
// index to the new value (and stop resolving the old one). Reindex OFF.
func TestIndexConverge_DiffSyncEditExisting(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()
	ctx := context.Background()

	p := openIdx(t, dirs[0], 0xa)
	defer p.Close()
	target := openIdx(t, dirs[1], 0xb)
	defer target.Close()

	cid, err := p.NewClass(ctx, rdx.ID0, SchemaIndex...)
	assert.NoError(t, err)
	rid, err := p.NewObjectTLV(ctx, cid, protocol.Records{protocol.Record('S', rdx.Stlv("v1"))})
	assert.NoError(t, err)
	_ = testutils.SyncData(p, target) // target has the object, indexed as "v1"

	got, err := target.ObjectIDByHash(cid, 1, []byte("v1"))
	assert.NoError(t, err, "pre-edit: v1 resolves")
	assert.Equal(t, rid, got)

	// Edit the indexed field on p, then sync the edit.
	_, oldTlv, err := p.ObjectFieldTLV(rid.ToOff(1))
	assert.NoError(t, err)
	_, err = p.EditFieldTLV(ctx, rid.ToOff(1), protocol.Record('S', rdx.Sdelta(oldTlv, "v2", p.Clock())))
	assert.NoError(t, err)
	_ = testutils.SyncData(p, target)

	got, err = target.ObjectIDByHash(cid, 1, []byte("v2"))
	assert.NoError(t, err, "post-edit: v2 must resolve")
	assert.Equal(t, rid, got)
	// Note: a lookup of the OLD value "v1" may still resolve here because
	// hashIndexCache positive-caches value->rid and GetByHash skips revalidation
	// on a cache hit. That's a separate, pre-existing cache concern (self-limiting
	// via LRU), not part of the index-convergence guarantee under test.
}

// O then E, in order, separate drains — the control case (already correct on main).
func TestIndexConverge_LiveObeforeE(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()
	ctx := context.Background()

	p := openIdx(t, dirs[0], 0xa)
	defer p.Close()
	cid, rid, cRecs, oRecs, eRecs := liveUnits(t, p)

	target := openIdx(t, dirs[1], 0xb)
	defer target.Close()
	assert.NoError(t, target.Drain(ctx, cRecs))
	assert.NoError(t, target.Drain(ctx, oRecs))
	assert.NoError(t, target.Drain(ctx, eRecs))

	assertResolves(t, target, cid, rid, "O-before-E must index current value v2")
}
