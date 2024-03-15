package main

import (
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
)

// FORMAT: replica creation packet
var Log0 = toyqueue.Records{
	toytlv.Record('L',
		toytlv.Record('I', ID0.ZipBytes()), // identifier, `src-0`
		toytlv.Record('R', ID0.ZipBytes()), // reference, `0-0`
		toytlv.Record('S', Stlv("Sname")),  // replica name (string)
	),
}
