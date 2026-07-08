package chotki

import (
	"context"
	"testing"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/replication"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/require"
)

// The batched drain accumulates Y/C/O/E records into one all-or-nothing pebble
// batch applied after the loop. An own-source record used to advance cho.last
// (the local id allocator) inside the loop, BEFORE that durable write. If a
// later record in the same batch errored, the batch — and its version-vector
// bumps — was dropped, but cho.last stayed advanced. After a crash cho.last is
// rebuilt from the persisted VV (which never saw the dropped op), so the next
// commit could reissue an id a peer already holds (divergence). The fix
// advances cho.last only after the batch is durably applied, so a dropped batch
// leaves the allocator exactly where the persisted VV left it.
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

	// The core invariant of the fix: cho.last must never sit ahead of the
	// persisted version vector. A crash rebuilds cho.last from the VV, so an id
	// beyond the VV would be reissued though a peer may already hold it. The
	// pre-fix code advanced cho.last inside the loop (before the durable write),
	// so a dropped batch left it ahead of the VV — this assertion fails there.
	require.False(t, vv.GetID(0x5c).Less(a.Last()),
		"cho.last must not run ahead of the persisted version vector")

	// And flush-on-error persisted the applied prefix, so the staged own-source
	// record is durable — its bump is reflected in the version vector rather
	// than silently dropped while still being rebroadcast to peers.
	require.False(t, vv.GetID(0x5c).Less(ownID),
		"the applied own-source prefix must be durable after a mid-batch error")
}
