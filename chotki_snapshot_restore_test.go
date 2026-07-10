package chotki

import (
	"context"
	"testing"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/require"
)

// After a diff-sync delivers our OWN history back to us — as happens when a
// replica restores from an older snapshot and a peer replays its tail — the
// synced version vector covers cho.src beyond the local allocator. cho.last
// must advance to that VV, or the next local commit reissues an id a peer
// already holds: two different mutations under one rdx.ID, cluster-wide
// divergence. The own history arrives via the handshake VV / diff
// batch, NOT as top-level own-source packets, so the maxOwn path (covered by
// TestCommitAllocatorSyncedWithOwnSourceDrain) never sees it — only the
// advance-from-merged-VV at 'V' closes the window.
func TestAllocatorAdvancesAfterOwnHistoryRestoredViaSync(t *testing.T) {
	dirs, clear := testdirs(0x5c)
	defer clear()
	a, err := Open(dirs[0], Options{Src: 0x5c, Name: "replica A"})
	require.NoError(t, err)
	defer a.Close()

	// A has only a little own history so far.
	_, err = a.NewClass(context.Background(), rdx.ID0, Schema...)
	require.NoError(t, err)
	before := a.Last()

	// A peer (0x99) that holds our (0x5c) history up to seq 100 hands it back via
	// a diff-sync. Its handshake VV claims 0x5c@100; ApplyH stages that VV into
	// the sync batch, which becomes durable at 'V'.
	ownHigh := rdx.IDFromSrcSeqOff(0x5c, 100, 0)
	require.True(t, before.Less(ownHigh))

	hid := rdx.IDFromSrcSeqOff(0x99, 7, 0)
	// VV values are pro, not seq — build via PutID so the encoded ids are exact.
	peerVV := rdx.VV{}
	peerVV.PutID(hid)
	peerVV.PutID(ownHigh)
	hpack := protocol.Record('H',
		protocol.TinyRecord('T', hid.ZipBytes()),
		protocol.Record('M', []byte{0}),
		protocol.Record('V', peerVV.TLV()),
	)
	// an empty diff closed immediately by its 'V' (no D-parcels needed to
	// exercise the allocator advance)
	vpack := protocol.Record('V', protocol.TinyRecord('T', hid.ZipBytes()))

	err = a.Drain(context.Background(), protocol.Records{hpack, vpack})
	require.NoError(t, err)

	vv, err := a.VersionVector()
	require.NoError(t, err)
	require.Equal(t, vv.GetID(0x5c), a.Last(),
		"cho.last must equal the synced VV[src] after own history is restored")
	require.False(t, a.Last().Less(ownHigh),
		"the reissue window is closed only if cho.last covers the restored history")
}
