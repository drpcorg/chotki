package chotki

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/replication"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/require"
)

// Regression tests for the replication protocol bugs found while writing
// the TLA+ spec of the sync protocol (see tla/README.md for the full
// stories). Each test fails against the pre-fix behaviour.

func newSyncer(host *Chotki, name string) *replication.Syncer {
	return &replication.Syncer{
		Src:           host.Source(),
		Host:          host,
		Mode:          replication.SyncRWLive,
		Name:          name,
		Log:           utils.NewDefaultLogger(slog.LevelError),
		PingWait:      time.Second,
		PingPeriod:    time.Minute,
		WaitUntilNone: time.Millisecond,
	}
}

func byeRecord(snaplast rdx.ID) []byte {
	return protocol.Record('B',
		protocol.TinyRecord('T', snaplast.ZipBytes()),
		[]byte("closing"))
}

func syncPointKeys(cho *Chotki) map[rdx.ID]bool {
	keys := make(map[rdx.ID]bool)
	cho.syncs.Range(func(key rdx.ID, _ *syncPoint) bool {
		keys[key] = true
		return true
	})
	return keys
}

// drainQueue reads records from an FDQueue until it has at least want
// records or the deadline passes.
func drainQueue(t *testing.T, q *utils.FDQueue[protocol.Records], want int, timeout time.Duration) protocol.Records {
	t.Helper()
	var got protocol.Records
	deadline := time.Now().Add(timeout)
	for len(got) < want && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		recs, _ := q.Feed(ctx)
		cancel()
		got = append(got, recs...)
	}
	return got
}

// Bug 1 (tla/README.md, MCBuggyRelay.cfg): a drained batch whose last
// record is 'B' (bye) — which network read batching routinely coalesces
// with the preceding data records — must still rebroadcast the applied
// records to the other sessions, minus the session-scoped trailing 'B'.
// Pre-fix, the whole batch was silently dropped from the relay, leaving
// downstream replicas permanently diverged.
func TestDrainRelaysAppliedRecordsWhenBatchEndsWithBye(t *testing.T) {
	dirs, clear := testdirs(0x51, 0x52)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0x51, Name: "replica A"})
	require.NoError(t, err)
	defer a.Close()
	b, err := Open(dirs[1], Options{Src: 0x52, Name: "replica B"})
	require.NoError(t, err)
	defer b.Close()

	// capture the packet replica A commits (as its live broadcast)
	qa := utils.NewFDQueue[protocol.Records](1<<20, 50*time.Millisecond, 1)
	a.outq.Store("capture", qa)
	cid, err := a.NewClass(context.Background(), rdx.ID0, Schema...)
	require.NoError(t, err)
	packets := drainQueue(t, qa, 1, 2*time.Second)
	require.Len(t, packets, 1)
	require.Equal(t, byte('C'), protocol.Lit(packets[0]))

	// replica B has a downstream session "c" that must receive relays
	qc := utils.NewFDQueue[protocol.Records](1<<20, 50*time.Millisecond, 1)
	b.outq.Store("c", qc)

	// the a—b session at B: handshake, then a batch of [C-packet, B(bye)]
	// coalesced the way network/peer.go keepRead does it
	sa, sb := newSyncer(a, "b"), newSyncer(b, "a")
	defer sa.Close()
	defer sb.Close()
	ha, err := sa.FeedHandshake()
	require.NoError(t, err)
	require.NoError(t, sb.Drain(context.Background(), ha))
	require.NoError(t, sb.Drain(context.Background(),
		protocol.Records{packets[0], byeRecord(a.Last())}))

	// B applied the class locally...
	_, err = b.ClassFields(cid)
	require.NoError(t, err)

	// ...and must have relayed the handshake and the class packet (but
	// not the bye) downstream
	relayed := drainQueue(t, qc, 2, 2*time.Second)
	require.Len(t, relayed, 2)
	require.Equal(t, byte('H'), protocol.Lit(relayed[0]))
	require.Equal(t, byte('C'), protocol.Lit(relayed[1]))
	for _, rec := range relayed {
		require.NotEqual(t, byte('B'), protocol.Lit(rec), "session-scoped bye must not be relayed")
	}
}

// Bug 2 (MCBuggyDisplace.cfg): a new handshake from a replica must
// displace that replica's older, unfinished sync points ("allow only 1
// diff sync per src"). Pre-fix the cleanup compared keys against the
// local src, which never matches, so stale sync points were never
// displaced.
func TestHandshakeDisplacesStaleSyncPointOfSameSrc(t *testing.T) {
	dirs, clear := testdirs(0x53, 0x54)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0x53, Name: "replica A"})
	require.NoError(t, err)
	defer a.Close()
	b, err := Open(dirs[1], Options{Src: 0x54, Name: "replica B"})
	require.NoError(t, err)
	defer b.Close()

	_, err = a.NewClass(context.Background(), rdx.ID0, Schema...)
	require.NoError(t, err)

	// session 1 from A dies mid-diff, leaving a stale sync point at B
	sa1, sb1 := newSyncer(a, "b"), newSyncer(b, "a")
	h1, err := sa1.FeedHandshake()
	require.NoError(t, err)
	k1 := a.Last()
	require.NoError(t, sb1.Drain(context.Background(), h1))
	require.True(t, syncPointKeys(b)[k1], "sync point for session 1 must exist")

	// A commits more (so its snapshot id changes) and re-handshakes
	_, err = a.NewClass(context.Background(), rdx.ID0, Schema...)
	require.NoError(t, err)
	sa2, sb2 := newSyncer(a, "b"), newSyncer(b, "a")
	defer sa2.Close()
	defer sb2.Close()
	h2, err := sa2.FeedHandshake()
	require.NoError(t, err)
	k2 := a.Last()
	require.NotEqual(t, k1, k2)
	require.NoError(t, sb2.Drain(context.Background(), h2))

	keys := syncPointKeys(b)
	require.True(t, keys[k2], "sync point for session 2 must exist")
	require.False(t, keys[k1], "stale sync point of the same src must be displaced")
}

// Bug 3 (MCBuggyStaleSync.cfg, found by TLC): a sync point must not
// outlive the session whose 'H' created it — the remaining 'D'/'V'
// records travel through that session's connection, so once it is gone
// the staged batch can never be completed legitimately, while a 'V'
// arriving through a newer connection would apply the staged handshake
// VV without the data (permanent silent data loss).
func TestSyncerCloseAbortsItsSyncPoints(t *testing.T) {
	dirs, clear := testdirs(0x55, 0x56)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0x55, Name: "replica A"})
	require.NoError(t, err)
	defer a.Close()
	b, err := Open(dirs[1], Options{Src: 0x56, Name: "replica B"})
	require.NoError(t, err)
	defer b.Close()

	_, err = a.NewClass(context.Background(), rdx.ID0, Schema...)
	require.NoError(t, err)

	sa, sb := newSyncer(a, "b"), newSyncer(b, "a")
	defer sa.Close()
	h, err := sa.FeedHandshake()
	require.NoError(t, err)
	k := a.Last()
	require.NoError(t, sb.Drain(context.Background(), h))
	require.True(t, syncPointKeys(b)[k])

	// the session dies mid-diff: its sync point must die with it
	require.NoError(t, sb.Close())
	require.False(t, syncPointKeys(b)[k], "sync point must be aborted with its session")

	// a late 'V' for that diff (e.g. relayed through a newer connection)
	// must fail loudly instead of poisoning the version vector
	_, vpack := protocol.OpenHeader(nil, 'V')
	vpack = append(vpack, protocol.Record('T', k.ZipBytes())...)
	protocol.CloseHeader(vpack, 5)
	err = b.Drain(context.Background(), protocol.Records{vpack})
	require.ErrorIs(t, err, ErrSyncUnknown)
}

// Bug 4 (tla/README.md item 11): a peer's bye only means it has nothing
// more to send; its drain side keeps applying records until the
// connection closes. The feed must therefore finish its own diff phase
// (through 'V') instead of cutting it short, which would strand a staged
// diff batch at the peer that silently misses the promised data.
func TestFeedFinishesDiffAfterPeerBye(t *testing.T) {
	dirs, clear := testdirs(0x57, 0x58)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0x57, Name: "replica A"})
	require.NoError(t, err)
	defer a.Close()
	b, err := Open(dirs[1], Options{Src: 0x58, Name: "replica B"})
	require.NoError(t, err)
	defer b.Close()

	cid, err := a.NewClass(context.Background(), rdx.ID0, Schema...)
	require.NoError(t, err)

	sa, sb := newSyncer(a, "b"), newSyncer(b, "a")
	defer sa.Close()
	defer sb.Close()

	// exchange handshakes
	ha, err := sa.Feed(context.Background())
	require.NoError(t, err)
	require.NoError(t, sb.Drain(context.Background(), ha))
	hb, err := sb.Feed(context.Background())
	require.NoError(t, err)
	require.NoError(t, sa.Drain(context.Background(), hb))

	// B (an empty, fast peer) says bye before A has fed its diff
	require.NoError(t, sa.Drain(context.Background(),
		protocol.Records{byeRecord(b.Last())}))

	// A must still deliver its complete diff, V packet included
	sawV := false
	for {
		recs, err := sa.Feed(context.Background())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		for _, rec := range recs {
			if protocol.Lit(rec) == 'V' {
				sawV = true
			}
		}
		require.NoError(t, sb.Drain(context.Background(), recs))
	}
	require.True(t, sawV, "feed must finish the diff (send 'V') after a peer bye")

	// and B must actually have the data A promised in its handshake
	_, err = b.ClassFields(cid)
	require.NoError(t, err, "peer must not be left without the promised diff data")
}

// ApplyD/ApplyV used to loop forever on truncated inner TLV records
// (protocol.Take makes no progress on incomplete input) and ApplyV could
// overwrite an earlier ErrBadVPacket with a later successful merge.
func TestApplyDVMalformedInputFailsFast(t *testing.T) {
	dirs, clear := testdirs(0x59)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0x59, Name: "replica A"})
	require.NoError(t, err)
	defer a.Close()

	batch := a.Database().NewBatch()
	defer batch.Close()

	// short-form headers claiming 200-byte bodies that are not there
	err = a.ApplyV(rdx.ID0, rdx.ID0, []byte{'v', 200}, batch)
	require.ErrorIs(t, err, ErrBadVPacket)
	err = a.ApplyD(rdx.ID0, rdx.ID0, []byte{'f', 200}, batch)
	require.ErrorIs(t, err, rdx.ErrBadPacket)
}
