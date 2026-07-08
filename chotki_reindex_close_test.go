package chotki

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/drpcorg/chotki/rdx"
	testutils "github.com/drpcorg/chotki/test_utils"
	"github.com/stretchr/testify/require"
)

// The reindex worker (CheckReindexTasks) spawns runReindexTask goroutines.
// They used to be untracked: Close cancelled the context and waited only for
// the worker itself, then closed pebble — a task still iterating/merging would
// panic ("pebble: closed"), crashing the whole test binary intermittently
// under load. Now the worker waits for its spawned tasks (taskWg) before
// returning, so Close never yanks the DB from under a running reindex.
func TestCloseWaitsForRunningReindexTasks(t *testing.T) {
	reindexPeriod = time.Millisecond
	t.Cleanup(func() { reindexPeriod = time.Second })

	for i := 0; i < 5; i++ {
		dirs, clear := testdirs(0xa, 0xb)

		a, err := Open(dirs[0], Options{Src: 0xa, Name: "replica A",
			ReadAccumTimeLimit: 100 * time.Millisecond})
		require.NoError(t, err)

		b, err := Open(dirs[1], Options{Src: 0xb, Name: "replica B",
			ReadAccumTimeLimit: 100 * time.Millisecond})
		require.NoError(t, err)

		cid, err := a.NewClass(context.Background(), rdx.ID0, SchemaIndex...)
		require.NoError(t, err)

		// enough hash-indexed objects that the receiver's reindex task has
		// real work in flight when we close it
		aorm := a.ObjectMapper()
		for j := 0; j < 64; j++ {
			obj := Test{Test: fmt.Sprintf("test-%d-%d", i, j)}
			require.NoError(t, aorm.New(context.Background(), cid, &obj))
		}
		aorm.UpdateAll()

		// the diff sync hands b's index work to its reindex tasks
		testutils.SyncData(a, b)

		// close immediately: pre-fix, a spawned runReindexTask could still be
		// mid-Merge here and panic on the closed DB
		require.NoError(t, aorm.Close())
		require.NoError(t, b.Close())
		require.NoError(t, a.Close())
		clear()
	}
}
