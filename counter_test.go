package chotki

import (
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCounterMerge(t *testing.T) {
	args := [][]byte{
		protocol.Concat(
			protocol.Record('I', rdx.IDFromString("b-345").ZipBytes()),
			protocol.Record('C', rdx.ZipZagInt64(2)),
			protocol.Record('I', rdx.IDFromString("a-123").ZipBytes()),
			protocol.Record('C', rdx.ZipZagInt64(1)),
		),
		protocol.Concat(
			protocol.Record('I', rdx.IDFromString("c-567").ZipBytes()),
			protocol.Record('C', rdx.ZipZagInt64(3)),
			protocol.Record('I', rdx.IDFromString("b-344").ZipBytes()),
			protocol.Record('C', rdx.ZipZagInt64(1)),
			protocol.Record('I', rdx.IDFromString("a-234").ZipBytes()),
			protocol.Record('C', rdx.ZipZagInt64(2)),
		),
	}
	result := CMerge(args)
	correct := protocol.Concat(
		protocol.Record('I', rdx.IDFromString("c-567").ZipBytes()),
		protocol.Record('C', rdx.ZipZagInt64(3)),
		protocol.Record('I', rdx.IDFromString("b-345").ZipBytes()),
		protocol.Record('C', rdx.ZipZagInt64(2)),
		protocol.Record('I', rdx.IDFromString("a-234").ZipBytes()),
		protocol.Record('C', rdx.ZipZagInt64(2)),
	)
	assert.Equal(t, correct, result)
}
