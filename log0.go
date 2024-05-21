package chotki

import (
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

const (
	id1 = rdx.ID0 + rdx.ProInc
	id2 = id1 + rdx.ProInc
	id3 = id2 + rdx.ProInc

	IdNames    = id2 + 1
	IdNodes    = id2 + 2
	IdNodeInfo = id2 + 3

	// ID from which we count user static objects
	IdLog1 = id2 + 4
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
			"MNames", // global-scope names
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
			"SName", // x-0-1 replica name
			"VAck",  // x-0-2 Packet acknowledgements (a vector)
			"SAddr", // x-0-3 the default IP address
			// todo the rest of metadata
		}),
	),
}
