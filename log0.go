package chotki

import (
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

var (
	id1 = rdx.ID0.IncPro(1)
	id2 = id1.IncPro(1)
	id3 = id2.IncPro(1)

	IdNames    = id2.ToOff(1)
	IdNodes    = id2.ToOff(2)
	IdNodeInfo = id2.ToOff(3)

	// ID from which we count user static objects
	IdLog1 = id2.ToOff(4)
)

const YAckOff = uint64(2)

var Log0 = protocol.Records{
	// 0-0 is the hardcoded read-only replica
	protocol.Record('Y',
		protocol.Record('I', rdx.ID0.ZipBytes()), // identifier, `src-0`
		protocol.Record('R', rdx.ID0.ZipBytes()), // reference, `0-0`
	),
	// 0-1 class declaration for the singleton metadata object
	protocol.Record('C',
		protocol.Record('I', id1.ZipBytes()),     // identifier, `src-0`
		protocol.Record('R', rdx.ID0.ZipBytes()), // reference, `0-0`
		rdx.Atlv(rdx.LogT{
			"M\x00Names", // global-scope names
		}),
	),
	// 0-2 the singleton metadata object
	protocol.Record('O',
		protocol.Record('I', id2.ZipBytes()),
		protocol.Record('R', id1.ZipBytes()),
		// 0-2-1 is the global names object (a map)
		protocol.Record('M',
			protocol.Record('T', rdx.Ttlv("0")),
			protocol.Record('R', rdx.Rtlv(rdx.ID0)),
			protocol.Record('T', rdx.Ttlv("Global")),
			protocol.Record('R', rdx.Rtlv(id2)),
			protocol.Record('T', rdx.Ttlv("Names")),
			protocol.Record('R', rdx.Rtlv(IdNames)),
			protocol.Record('T', rdx.Ttlv("Nodes")),
			protocol.Record('R', rdx.Rtlv(IdNodes)),
		),
	),
	// 0-3 class declaration for replica objects,
	// i.e. objects x-0 for every replica x
	protocol.Record('C',
		protocol.Record('I', id3.ZipBytes()),
		protocol.Record('R', rdx.ID0.ZipBytes()),
		rdx.Atlv(rdx.LogT{
			"S\x00Name", // x-0-1 replica name
			"V\x00Ack",  // x-0-2 Packet acknowledgements (a vector)
			"S\x00Addr", // x-0-3 the default IP address
			// todo the rest of metadata
		}),
	),
}
