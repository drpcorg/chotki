package chotki

import (
	"context"
	"testing"

	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/require"
)

// An N-counter must not lose increments when a second writer (AddToNField)
// touches the same slot between the manager's read and its flush.
// Without the reload-rebase + read-modify-write flush, the manager writes the
// absolute cached mine, which loses to the max-by-source merge, and never
// adopts the external delta — so Get() under-reports permanently.
func TestNCounterSurvivesConcurrentExternalWriter(t *testing.T) {
	dirs, clear := testdirs(0x6e)
	defer clear()
	a, err := Open(dirs[0], Options{Src: 0x6e, Name: "A"})
	require.NoError(t, err)
	defer a.Close()

	cid, err := a.NewClass(context.Background(), rdx.ID0,
		classes.Field{Name: "n", RdxType: rdx.Natural})
	require.NoError(t, err)
	oid, err := a.NewObjectTLV(context.Background(), cid, protocol.Records{
		protocol.Record(rdx.Natural, rdx.Ntlvt(0, a.Source())),
	})
	require.NoError(t, err)
	fid := oid.ToOff(1)

	c := a.Counter(oid, 1)
	_, err = c.Increment(context.Background(), 1) // manager cache mine=1, unflushed
	require.NoError(t, err)

	// External writer raises the same slot before the manager flushes.
	_, err = a.AddToNField(context.Background(), fid, 9)
	require.NoError(t, err)

	a.SyncCounters(context.Background()) // flush (RMW) + reload

	got, err := c.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(10), got,
		"N-counter must reflect both the external +9 and the local +1")
}
