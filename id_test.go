package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCases(t *testing.T) {
	off := ParseID("0ff")
	assert.Equal(t, uint64(off), uint64(0xff))

	zip := ID(1).ZipBytes()
	assert.Equal(t, ID(1), UnzipID(zip))
}

func TestParseID(t *testing.T) {
	ids := []string{
		"0",
		"3",
		"fa3-57",
		"fffff-ffffffff-fff",
	}
	for _, str := range ids {
		id := ParseID(str)
		assert.NotEqual(t, BadId, id)
		str2 := id.String()
		assert.Equal(t, str, str2)
		fullstr := string(id.Hex583(nil))
		id2 := ParseID(fullstr)
		assert.Equal(t, id, id2)
	}
}

func TestFieldNameType(t *testing.T) {
	src := uint32(0x8e)
	seq := uint32(0x82f0)
	id := MakeID(src, seq, ExampleName)
	assert.Equal(t, "8e-82f0-32", id.String())
}
