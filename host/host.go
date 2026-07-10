package host

import (
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
)

// Edit is an 'E' edit to object Ref, with Body as [F(offset), <value>] records.
type Edit struct {
	Ref  rdx.ID
	Body protocol.Records
}

type Host interface {
	ClassFields(cid rdx.ID) (fields classes.Fields, err error)
	GetFieldTLV(id rdx.ID) (rdt byte, tlv []byte)
	Logger() utils.Logger
	Last() rdx.ID
	Source() uint64
	WriteOptions() *pebble.WriteOptions
	Database() *pebble.DB
	ObjectFieldTLV(fid rdx.ID) (rdt byte, tlv []byte, err error)
	CommitPacket(ctx context.Context, lit byte, ref rdx.ID, body protocol.Records) (id rdx.ID, err error)
	// CommitBatch commits edits in slice order, all-or-nothing, in a single Pebble batch and broadcast.
	CommitBatch(ctx context.Context, edits []Edit) (err error)
	Broadcast(ctx context.Context, records protocol.Records, except string)
	Drain(ctx context.Context, recs protocol.Records) (err error)
	// DrainApplied is Drain plus the count of applied records (0 on error,
	// len(recs) on success, since draining is all-or-nothing).
	DrainApplied(ctx context.Context, recs protocol.Records) (applied int, err error)
	// AbortSyncsVia closes the pending diff-sync points created by the session.
	AbortSyncsVia(ctx context.Context, sessionId string)
	Snapshot() pebble.Reader
	// StartSequentialWrite/EndSequentialWrite bracket a read-modify-write (a
	// Z-counter flush/"set" reads a slot and writes value@rev+1): held across
	// both read and commit, two writers of one slot serialize instead of
	// colliding on the revision. Cooperative (commit methods don't take it; the
	// read must be inside the bracket), not reentrant.
	StartSequentialWrite()
	EndSequentialWrite()
}
