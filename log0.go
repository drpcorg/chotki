package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
	"github.com/drpcorg/chotki/toytlv"
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
var Log0 = utils.Records{
	toytlv.Record('Y',
		toytlv.Record('I', rdx.ID0.ZipBytes()), // identifier, `src-0`
		toytlv.Record('R', rdx.ID0.ZipBytes()), // reference, `0-0`
		toytlv.Record('T', rdx.Ttlv("Name")),   // replica Name (string)
		toytlv.Record('T', rdx.Ttlv("S")),
	),
	toytlv.Record('C',
		toytlv.Record('I', id1.ZipBytes()),     // identifier, `src-0`
		toytlv.Record('R', rdx.ID0.ZipBytes()), // reference, `0-0`
		toytlv.Record('T', rdx.Ttlv("Names")),  // global-scope names
		toytlv.Record('T', rdx.Ttlv("M")),
		toytlv.Record('T', rdx.Ttlv("Nodes")), // replica addresses
		toytlv.Record('T', rdx.Ttlv("M")),
		toytlv.Record('T', rdx.Ttlv("NodeInfo")), // replica addresses
		toytlv.Record('T', rdx.Ttlv("M")),
	),
	toytlv.Record('O',
		toytlv.Record('I', id2.ZipBytes()),
		toytlv.Record('R', id1.ZipBytes()),
		toytlv.Record('M',
			toytlv.Record('T', rdx.Ttlv("0")),
			toytlv.Record('R', rdx.Rtlv(rdx.ID0)),
			toytlv.Record('T', rdx.Ttlv("Global")),
			toytlv.Record('R', rdx.Rtlv(id2)),
			toytlv.Record('T', rdx.Ttlv("Names")),
			toytlv.Record('R', rdx.Rtlv(IdNames)),
			toytlv.Record('T', rdx.Ttlv("Nodes")),
			toytlv.Record('R', rdx.Rtlv(IdNodes)),
		),
		toytlv.Record('M'),
		toytlv.Record('M'),
	),
}
