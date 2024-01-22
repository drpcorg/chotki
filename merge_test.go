package main

import (
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/stretchr/testify/assert"
	"testing"
)

const VVName = (1 << FieldTypeBits) | ('V' - 'A')

func TestVMerge(t *testing.T) {
	args := [][]byte{
		toytlv.Concat(
			toytlv.Record('V', ParseID("b-345").ZipBytes()),
			toytlv.Record('V', ParseID("a-123").ZipBytes()),
		),
		toytlv.Concat(
			toytlv.Record('V', ParseID("c-567").ZipBytes()),
			toytlv.Record('V', ParseID("b-344").ZipBytes()),
			toytlv.Record('V', ParseID("a-234").ZipBytes()),
		),
	}
	//result := VMerge(args)
	ma := PebbleMergeAdaptor{
		key: OKey(ID(0).ToOff(VVName)),
	}
	_ = ma.MergeOlder(args[0])
	_ = ma.MergeNewer(args[1])
	actual, _, _ := ma.Finish(true)
	correct := toytlv.Concat(
		toytlv.Record('V', ParseID("c-567").ZipBytes()),
		toytlv.Record('V', ParseID("b-345").ZipBytes()),
		toytlv.Record('V', ParseID("a-234").ZipBytes()),
	)
	assert.Equal(t, correct, actual)

}
