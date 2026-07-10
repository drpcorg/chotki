package chotki

import (
	"context"
	"testing"
	"time"

	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/replication"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/require"
)

// The batched drain accumulates Y/C/O/E records into one pebble batch applied
// after the loop. On ANY mid-drain error the whole top-level batch is now
// dropped — no partial prefix is ever applied (drop-on-error). So a batch that
// errors partway persists nothing and leaves the allocator exactly where the
// persisted version vector left it: cho.last can never lead the VV, and a
// crash/restart rebuilds cho.last from that same VV. (This reverses the earlier
// flush-on-error behavior, which persisted the applied prefix; see the
// review-fix design doc for the rationale — cluster ①.)
func TestDrainErrorDoesNotOverrunAllocator(t *testing.T) {
	dirs, clear := testdirs(0x5c)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0x5c, Name: "replica A"})
	require.NoError(t, err)
	defer a.Close()

	// capture a real 'C' packet to reuse its class body in a well-formed
	// own-source record below
	qa := utils.NewFDQueue[protocol.Records](1<<20, 50*time.Millisecond, 1)
	a.outq.Store("capture", qa)
	_, err = a.NewClass(context.Background(), rdx.ID0, Schema...)
	require.NoError(t, err)
	packets := drainQueue(t, qa, 1, 2*time.Second)
	require.Len(t, packets, 1)
	a.outq.Delete("capture")
	_, _, ref, body, err := replication.ParsePacket(packets[0])
	require.NoError(t, err)

	before := a.Last()

	// a well-formed own-source 'C' stamped well ahead of cho.last ...
	ownID := rdx.IDFromSrcSeqOff(0x5c, before.Seq()+1000, 0)
	require.True(t, before.Less(ownID))
	ownC := protocol.Record('C',
		protocol.Record('I', ownID.ZipBytes()),
		protocol.Record('R', ref.ZipBytes()),
		body)
	// ... followed by a 'V' for a sync point that does not exist, which aborts
	// the drain with ErrSyncUnknown after the own-source record was staged.
	_, vpack := protocol.OpenHeader(nil, 'V')
	unknown := rdx.IDFromSrcSeqOff(0x99, 7, 0)
	vpack = append(vpack, protocol.Record('T', unknown.ZipBytes())...)
	protocol.CloseHeader(vpack, 5)

	err = a.Drain(context.Background(), protocol.Records{ownC, vpack})
	require.ErrorIs(t, err, ErrSyncUnknown)

	vv, err := a.VersionVector()
	require.NoError(t, err)

	// New semantics: on ANY mid-drain error the top-level batch is dropped
	// wholesale — no partial prefix is ever applied. So the staged own-source
	// record is NOT durable, and the allocator did not move.
	require.True(t, vv.GetID(0x5c).Less(ownID),
		"a dropped batch must not persist the staged own-source record")
	require.Equal(t, before, a.Last(),
		"the allocator must not advance when the batch is dropped")

	// The core invariant still holds (now trivially): cho.last never sits ahead
	// of the persisted version vector.
	require.False(t, vv.GetID(0x5c).Less(a.Last()),
		"cho.last must not run ahead of the persisted version vector")
}

// A CommitBatch whose drain errors midway must persist NOTHING (all-or-nothing),
// so a caller that retries the whole batch cannot double-apply a durable prefix
// (the Z-counter read-modify-write doubling).
func TestCommitBatchIsAllOrNothingOnError(t *testing.T) {
	dirs, clear := testdirs(0x5d)
	defer clear()
	a, err := Open(dirs[0], Options{Src: 0x5d, Name: "replica A"})
	require.NoError(t, err)
	defer a.Close()

	cid, err := a.NewClass(context.Background(), rdx.ID0, Schema...)
	require.NoError(t, err)
	oid, err := a.NewObjectTLV(context.Background(), cid, protocol.Records{
		protocol.Record(rdx.String, rdx.Stlv("v0")),
	})
	require.NoError(t, err)

	// Persisted VV[src] is the reliable "durable?" signal. We do NOT assert on
	// a.Last(): CommitBatch pre-allocates ids via nextLast() BEFORE draining, so
	// the in-memory allocator advances even when the batch is dropped (an
	// accepted local-commit gap — the ids were never broadcast).
	vvBefore, err := a.VersionVector()
	require.NoError(t, err)

	// A good edit followed by an edit whose field offset is out of range, which
	// makes ApplyE return ErrBadEPacket AFTER the good edit merged into pb.
	// ErrBadEPacket is deterministic (packets.go: field > rdx.OffMask).
	good := host.Edit{Ref: oid, Body: protocol.Records{
		protocol.Record('F', rdx.ZipUint64(1)),
		protocol.Record(rdx.String, rdx.Stlv("v1")),
	}}
	bad := host.Edit{Ref: oid, Body: protocol.Records{
		protocol.Record('F', rdx.ZipUint64(uint64(rdx.OffMask)+1)),
		protocol.Record(rdx.String, rdx.Stlv("v2")),
	}}

	err = a.CommitBatch(context.Background(), []host.Edit{good, bad})
	require.Error(t, err)

	// Nothing durable: the batch was dropped wholesale, so the persisted version
	// vector did not advance to cover the good edit. (The good edit's ApplyE
	// merged a VKey bump into pb, which drop-on-error discards.)
	vvAfter, err := a.VersionVector()
	require.NoError(t, err)
	require.Equal(t, vvBefore.GetID(0x5d), vvAfter.GetID(0x5d),
		"a failed CommitBatch must not persist its applied prefix")
}
