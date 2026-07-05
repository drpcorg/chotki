package replication

import (
	"context"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/require"
)

func testSyncer() *Syncer {
	return &Syncer{
		Name: "test",
		Log:  utils.NewDefaultLogger(slog.LevelError),
	}
}

// processPings used to remove 'P' records while ranging over the same
// slice, which skipped the record that followed a removed one: in a
// coalesced [ping, X] batch, X escaped ping processing and a stray 'P'
// could survive into the relayed records.
func TestProcessPingsFiltersAllPingsAndKeepsTheRest(t *testing.T) {
	sync := testSyncer()

	ping := protocol.Record('P', rdx.Stlv(PingVal))
	pong := protocol.Record('P', rdx.Stlv(PongVal))
	e1 := protocol.Record('E', []byte("one"))
	e2 := protocol.Record('E', []byte("two"))

	out := sync.processPings(protocol.Records{e1, ping, e2, pong})
	require.Equal(t, protocol.Records{e1, e2}, out)
	require.Equal(t, int32(Pong), sync.pingStage.Load(),
		"a received ping must schedule a pong")

	sync = testSyncer()
	out = sync.processPings(protocol.Records{ping, pong})
	require.Empty(t, out, "consecutive pings must all be filtered")
}

// WaitDrainState used to leak its watcher goroutine forever when given a
// non-cancellable context (Feed's SendNone path passes
// context.Background()), one goroutine per closed session.
func TestWaitDrainStateDoesNotLeakGoroutines(t *testing.T) {
	sync := testSyncer()

	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		ch := sync.WaitDrainState(context.Background(), SendDiff)
		sync.SetDrainState(context.Background(), SendDiff)
		<-ch
		sync.SetDrainState(context.Background(), SendHandshake)
	}

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before+5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before+5,
		"WaitDrainState goroutines must terminate once the wait completes")
}

// The result channel is buffered now: a caller that abandons the wait on
// timeout (Feed's SendDiff select) must not strand the waiter goroutine
// on the send.
func TestWaitDrainStateAbandonedWaitDoesNotLeak(t *testing.T) {
	sync := testSyncer()

	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		_ = sync.WaitDrainState(ctx, SendDiff) // never read
		sync.SetDrainState(context.Background(), SendDiff)
		cancel()
		sync.SetDrainState(context.Background(), SendHandshake)
	}

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before+5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before+5,
		"abandoned WaitDrainState waiters must terminate")
}
