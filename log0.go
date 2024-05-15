package chotki

import (
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

const (
	id1 = rdx.ID0 + rdx.ProInc
	id2 = id1 + rdx.ProInc

	IdNames    = id2 + 1
	IdNodes    = id2 + 2
	IdNodeInfo = id2 + 3

	// ID from which we count user static objects
	IdLog1 = id2 + 4
)

// FORMAT: replica creation packet
var Log0 = protocol.Records{
	protocol.Record('Y',
		protocol.Record('I', rdx.ID0.ZipBytes()), // identifier, `src-0`
		protocol.Record('R', rdx.ID0.ZipBytes()), // reference, `0-0`
		protocol.Record('T', rdx.Ttlv("Name")),   // replica Name (string)
		protocol.Record('T', rdx.Ttlv("S")),
	),
	protocol.Record('C',
		protocol.Record('I', id1.ZipBytes()),     // identifier, `src-0`
		protocol.Record('R', rdx.ID0.ZipBytes()), // reference, `0-0`
		rdx.Atlv(rdx.LogT{
			"MNames", // global-scope names
			"MNodes", // replica addresses
			"MNodeInfo",
		}),
	),
	protocol.Record('O',
		protocol.Record('I', id2.ZipBytes()),
		protocol.Record('R', id1.ZipBytes()),
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
		protocol.Record('M'),
		protocol.Record('M'),
	),
}
