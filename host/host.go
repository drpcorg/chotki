package host

import (
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
)

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
	Broadcast(ctx context.Context, records protocol.Records, except string)
	Drain(ctx context.Context, recs protocol.Records) (err error)
	Snapshot() pebble.Reader
}
