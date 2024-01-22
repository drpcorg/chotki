package main

import (
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLWWMergeString(t *testing.T) {
	args := [][]byte{
		toytlv.Concat(
			toytlv.Record('I', ParseID("345-b").ZipBytes()),
			toytlv.Record('S',
				toytlv.Record('T', ZipUint64(1)),
				[]byte("hello world 1"),
			),
		),
		toytlv.Concat(
			toytlv.Record('I', ParseID("45-d").ZipBytes()),
			toytlv.Record('S',
				toytlv.Record('T', ZipUint64(3)),
				[]byte("hello world 3"),
			),
		),
		toytlv.Concat(
			toytlv.Record('I', ParseID("123-c").ZipBytes()),
			toytlv.Record('S',
				toytlv.Record('T', ZipUint64(2)),
				[]byte("hello world 2"),
			),
		),
	}
	result := LMerge(args)
	correct := toytlv.Concat(
		toytlv.Record('I', ParseID("45-d").ZipBytes()),
		toytlv.Record('S',
			toytlv.Record('T', ZipUint64(3)),
			[]byte("hello world 3"),
		),
	)
	assert.Equal(t, correct, result)
}

func TestLWWMergeInt(t *testing.T) {
	args := [][]byte{
		toytlv.Concat(
			toytlv.Record('I', ParseID("345-b").ZipBytes()),
			toytlv.Record('U',
				toytlv.Record('T', ZipUint64(1)),
				ZipUint64(1),
			),
		),
		toytlv.Concat(
			toytlv.Record('I', ParseID("45-d").ZipBytes()),
			toytlv.Record('U',
				toytlv.Record('T', ZipUint64(3)),
				ZipUint64(3),
			),
		),
		toytlv.Concat(
			toytlv.Record('I', ParseID("123-c").ZipBytes()),
			toytlv.Record('U',
				toytlv.Record('T', ZipUint64(2)),
				ZipUint64(2),
			),
		),
	}
	result := LMerge(args)
	correct := toytlv.Concat(
		toytlv.Record('I', ParseID("45-d").ZipBytes()),
		toytlv.Record('U',
			toytlv.Record('T', ZipUint64(3)),
			ZipUint64(3),
		),
	)
	assert.Equal(t, correct, result)
}
