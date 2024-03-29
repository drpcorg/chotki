package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCounterMerge(t *testing.T) {
	args := [][]byte{
		toytlv.Concat(
			toytlv.Record('I', rdx.ParseIDString("b-345").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(2)),
			toytlv.Record('I', rdx.ParseIDString("a-123").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(1)),
		),
		toytlv.Concat(
			toytlv.Record('I', rdx.ParseIDString("c-567").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(3)),
			toytlv.Record('I', rdx.ParseIDString("b-344").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(1)),
			toytlv.Record('I', rdx.ParseIDString("a-234").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(2)),
		),
	}
	result := CMerge(args)
	correct := toytlv.Concat(
		toytlv.Record('I', rdx.ParseIDString("c-567").ZipBytes()),
		toytlv.Record('C', rdx.ZipZagInt64(3)),
		toytlv.Record('I', rdx.ParseIDString("b-345").ZipBytes()),
		toytlv.Record('C', rdx.ZipZagInt64(2)),
		toytlv.Record('I', rdx.ParseIDString("a-234").ZipBytes()),
		toytlv.Record('C', rdx.ZipZagInt64(2)),
	)
	assert.Equal(t, correct, result)
}
