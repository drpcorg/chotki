package chotki

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/require"
)

// failMergeWriter wraps a real batch but fails Merge for object ('O'-prefixed)
// keys, so we can verify ApplyD surfaces the Merge error instead of the
// else-branch OnFieldUpdate call reassigning err to nil.
type failMergeWriter struct {
	*pebble.Batch
}

func (w failMergeWriter) Merge(key, value []byte, o *pebble.WriteOptions) error {
	if len(key) > 0 && key[0] == 'O' {
		return errors.New("injected merge failure")
	}
	return w.Batch.Merge(key, value, o)
}

func TestApplyDReturnsMergeError(t *testing.T) {
	dirs, clear := testdirs(0x71)
	defer clear()
	a, err := Open(dirs[0], Options{Src: 0x71, Name: "A"})
	require.NoError(t, err)
	defer a.Close()

	batch := a.db.NewBatch()
	defer batch.Close()
	ref := rdx.IDFromSrcSeqOff(0x71, 1, 0)
	// one D-parcel: F(offset) + an 'S' (FIRST) record, so the else-branch runs
	body := append(
		protocol.Record('F', rdx.ZipUint64(1)),
		protocol.Record('S', rdx.Stlv("x"))...,
	)
	var created []rdx.ID
	err = a.ApplyD(ref, ref, body, failMergeWriter{batch}, &created, nil)
	require.Error(t, err, "ApplyD must return the batch.Merge error, not swallow it")
}
