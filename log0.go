package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
)

const id1 = rdx.ID0 + rdx.ProInc
const ID2 = id1 + rdx.ProInc
const NamesID = ID2 + 1
const NodesID = ID2 + 2
const NodeInfoID = ID2 + 3

// FORMAT: replica creation packet
var Log0 = toyqueue.Records{
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
		toytlv.Record('I', ID2.ZipBytes()),
		toytlv.Record('R', id1.ZipBytes()),
		toytlv.Record('M',
			toytlv.Record('T', rdx.Ttlv("0")),
			toytlv.Record('R', rdx.Rtlv(rdx.ID0)),
			toytlv.Record('T', rdx.Ttlv("Global")),
			toytlv.Record('R', rdx.Rtlv(ID2)),
			toytlv.Record('T', rdx.Ttlv("Names")),
			toytlv.Record('R', rdx.Rtlv(ID2+1)),
			toytlv.Record('T', rdx.Ttlv("Nodes")),
			toytlv.Record('R', rdx.Rtlv(ID2+2)),
		),
		toytlv.Record('M'),
		toytlv.Record('M'),
	),
}
