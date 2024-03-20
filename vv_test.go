package main

import (
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVMerge(t *testing.T) {
	args := [][]byte{
		toytlv.Concat(
			toytlv.Record('V', ParseIDString("b-345").ZipBytes()),
			toytlv.Record('V', ParseIDString("a-123").ZipBytes()),
		),
		toytlv.Concat(
			toytlv.Record('V', ParseIDString("c-567").ZipBytes()),
			toytlv.Record('V', ParseIDString("b-344").ZipBytes()),
			toytlv.Record('V', ParseIDString("a-234").ZipBytes()),
		),
	}
	//result := Vmerge(args)
	ma := PebbleMergeAdaptor{
		id: ID0,
	}
	_ = ma.MergeOlder(args[0])
	_ = ma.MergeNewer(args[1])
	actual, _, _ := ma.Finish(true)
	correct := toytlv.Concat(
		toytlv.Record('V', ParseIDString("c-567").ZipBytes()),
		toytlv.Record('V', ParseIDString("b-345").ZipBytes()),
		toytlv.Record('V', ParseIDString("a-234").ZipBytes()),
	)
	assert.Equal(t, correct, actual)

}
