package chotki

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/replication"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/require"
)

// Concurrent drains are not serialized (Drain holds only an RLock), so an 'E'
// editing a hash-indexed field can race the 'O' creating its object on another
// session. Pre-fix, the E's index update was silently skipped when the object
// was not yet visible (class unresolvable -> ID0 -> "not indexed"), and if the
// O's post-commit reindex also ran before the E committed, the index stayed
// stale forever (no reindex task is scheduled for that case). Now ApplyE
// defers such edits to the same post-commit IndexObject pass as created
// objects: whichever of E/O commits last reindexes from the merged state.
func TestIndexConverge_ConcurrentEO(t *testing.T) {
	dirs, clear := testdirs(0xa)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0xa, Name: "replica A"})
	require.NoError(t, err)
	defer a.Close()

	// capture the C, O and E packets off replica A
	q := utils.NewFDQueue[protocol.Records](1<<20, 50*time.Millisecond, 1)
	a.outq.Store("capture", q)
	defer a.outq.Delete("capture")

	cid, err := a.NewClass(context.Background(), rdx.ID0, SchemaIndex...)
	require.NoError(t, err)
	aorm := a.ObjectMapper()
	defer aorm.Close()
	obj := Test{Test: "v1"}
	require.NoError(t, aorm.New(context.Background(), cid, &obj))
	aorm.UpdateAll()
	obj.Test = "v2"
	require.NoError(t, aorm.Save(context.Background(), &obj))
	aorm.UpdateAll()

	packets := drainQueue(t, q, 3, 5*time.Second)
	require.Len(t, packets, 3)
	var cPack, oPack, ePack []byte
	for _, p := range packets {
		lit, _, _, _, perr := replication.ParsePacket(p)
		require.NoError(t, perr)
		switch lit {
		case 'C':
			cPack = p
		case 'O':
			oPack = p
		case 'E':
			ePack = p
		}
	}
	require.NotNil(t, cPack)
	require.NotNil(t, oPack)
	require.NotNil(t, ePack)

	// replay the O and E concurrently into fresh replicas; the hash index
	// must always converge to the edited value
	for round := 0; round < 8; round++ {
		cdirs, cclear := testdirs(0xc)
		c, err := Open(cdirs[0], Options{Src: 0xc, Name: "replica C"})
		require.NoError(t, err)

		require.NoError(t, c.Drain(context.Background(), protocol.Records{cPack}))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = c.Drain(context.Background(), protocol.Records{oPack})
		}()
		go func() {
			defer wg.Done()
			_ = c.Drain(context.Background(), protocol.Records{ePack})
		}()
		wg.Wait()

		corm := c.ObjectMapper()
		got, err := waitGetByHash[*Test](t, corm, cid, 1, []byte("v2"), 10*time.Second)
		require.NoError(t, err, "round %d: hash index must converge to the edited value", round)
		require.Equal(t, "v2", got.Test, "round %d", round)

		require.NoError(t, corm.Close())
		require.NoError(t, c.Close())
		cclear()
	}
}
