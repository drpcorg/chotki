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
	// DrainApplied works like Drain but also reports how many records of
	// the batch were fully applied before an error stopped processing.
	DrainApplied(ctx context.Context, recs protocol.Records) (applied int, err error)
	// AbortSyncsVia closes and removes the pending diff-sync points
	// created by the given replication session.
	AbortSyncsVia(ctx context.Context, sessionId string)
	Snapshot() pebble.Reader
	// StartSequentialWrite/EndSequentialWrite bracket a read-modify-write:
	// writers whose op depends on the current DB state (a Z-counter flush or
	// "set" reads the slot and writes value@rev+1) take the bracket around
	// BOTH the read and the commit. Two bracketed writers of one slot then
	// serialize instead of racing — without it, one lands between the other's
	// read and commit, the two ops collide on the revision, and the merge
	// silently drops one of them.
	//
	// The bracket is cooperative: commit methods do NOT take it, and writers
	// that skip it are not serialized. Reads must happen INSIDE the bracket —
	// wrapping a commit of a stale-computed op fixes nothing. Not reentrant.
	StartSequentialWrite()
	EndSequentialWrite()
}
