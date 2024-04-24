package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/toytlv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCounterMerge(t *testing.T) {
	args := [][]byte{
		toytlv.Concat(
			toytlv.Record('I', rdx.IDFromString("b-345").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(2)),
			toytlv.Record('I', rdx.IDFromString("a-123").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(1)),
		),
		toytlv.Concat(
			toytlv.Record('I', rdx.IDFromString("c-567").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(3)),
			toytlv.Record('I', rdx.IDFromString("b-344").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(1)),
			toytlv.Record('I', rdx.IDFromString("a-234").ZipBytes()),
			toytlv.Record('C', rdx.ZipZagInt64(2)),
		),
	}
	result := CMerge(args)
	correct := toytlv.Concat(
		toytlv.Record('I', rdx.IDFromString("c-567").ZipBytes()),
		toytlv.Record('C', rdx.ZipZagInt64(3)),
		toytlv.Record('I', rdx.IDFromString("b-345").ZipBytes()),
		toytlv.Record('C', rdx.ZipZagInt64(2)),
		toytlv.Record('I', rdx.IDFromString("a-234").ZipBytes()),
		toytlv.Record('C', rdx.ZipZagInt64(2)),
	)
	assert.Equal(t, correct, result)
}
