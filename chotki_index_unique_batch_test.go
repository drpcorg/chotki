package chotki

import (
	"context"
	"testing"

	"github.com/drpcorg/chotki/chotki_errors"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/require"
)

// Two edits in one CommitBatch setting the same unique hash-indexed value on
// different objects must be rejected before the batch commits. The
// uniqueness gate reads committed state, so without the per-drain claims map the
// two writes never see each other and both land, producing a forbidden multi-id
// index set.
func TestHashUniqueViolationWithinBatch(t *testing.T) {
	dirs, clear := testdirs(0x7c)
	defer clear()
	a, err := Open(dirs[0], Options{Src: 0x7c, Name: "A"})
	require.NoError(t, err)
	defer a.Close()

	cid, err := a.NewClass(context.Background(), rdx.ID0,
		classes.Field{Name: "u", RdxType: rdx.String, Index: classes.HashIndex})
	require.NoError(t, err)

	o1, err := a.NewObjectTLV(context.Background(), cid, protocol.Records{
		protocol.Record(rdx.String, rdx.Stlv("alice")),
	})
	require.NoError(t, err)
	o2, err := a.NewObjectTLV(context.Background(), cid, protocol.Records{
		protocol.Record(rdx.String, rdx.Stlv("bob")),
	})
	require.NoError(t, err)

	vvBefore, err := a.VersionVector()
	require.NoError(t, err)

	// one batch sets BOTH objects' unique field to the same value
	dupEdit := func(oid rdx.ID) host.Edit {
		return host.Edit{Ref: oid, Body: protocol.Records{
			protocol.Record('F', rdx.ZipUint64(1)),
			protocol.Record(rdx.String, rdx.Stlv("carol")),
		}}
	}
	err = a.CommitBatch(context.Background(), []host.Edit{dupEdit(o1), dupEdit(o2)})
	require.ErrorIs(t, err, chotki_errors.ErrHashIndexUinqueConstraintViolation,
		"a duplicate unique value within one batch must be rejected")

	// drop-on-error: the rejected batch persists nothing, so the version vector
	// did not advance.
	vvAfter, err := a.VersionVector()
	require.NoError(t, err)
	require.Equal(t, vvBefore.GetID(0x7c), vvAfter.GetID(0x7c),
		"a rejected batch must persist nothing")
}
