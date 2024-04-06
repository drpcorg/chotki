package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
)

// FORMAT: replica creation packet
var Log0 = toyqueue.Records{
	toytlv.Record('Y',
		toytlv.Record('I', rdx.ID0.ZipBytes()), // identifier, `src-0`
		toytlv.Record('R', rdx.ID0.ZipBytes()), // reference, `0-0`
		toytlv.Record('S', rdx.Stlv("Sname")),  // replica Name (string)
	),
}
