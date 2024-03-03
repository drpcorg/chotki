package main

import (
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCounterMerge(t *testing.T) {
	args := [][]byte{
		toytlv.Concat(
			toytlv.Record('I', ParseIDString("b-345").ZipBytes()),
			toytlv.Record('C', ZipZagInt64(2)),
			toytlv.Record('I', ParseIDString("a-123").ZipBytes()),
			toytlv.Record('C', ZipZagInt64(1)),
		),
		toytlv.Concat(
			toytlv.Record('I', ParseIDString("c-567").ZipBytes()),
			toytlv.Record('C', ZipZagInt64(3)),
			toytlv.Record('I', ParseIDString("b-344").ZipBytes()),
			toytlv.Record('C', ZipZagInt64(1)),
			toytlv.Record('I', ParseIDString("a-234").ZipBytes()),
			toytlv.Record('C', ZipZagInt64(2)),
		),
	}
	result := CMerge(args)
	correct := toytlv.Concat(
		toytlv.Record('I', ParseIDString("c-567").ZipBytes()),
		toytlv.Record('C', ZipZagInt64(3)),
		toytlv.Record('I', ParseIDString("b-345").ZipBytes()),
		toytlv.Record('C', ZipZagInt64(2)),
		toytlv.Record('I', ParseIDString("a-234").ZipBytes()),
		toytlv.Record('C', ZipZagInt64(2)),
	)
	assert.Equal(t, correct, result)
}
