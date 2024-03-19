package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCases(t *testing.T) {
	off := ParseIDString("0ff")
	assert.Equal(t, uint64(off), uint64(0xff))

	zip := id64(1).ZipBytes()
	assert.Equal(t, id64(1), IDFromZipBytes(zip))
}

func TestParseID(t *testing.T) {
	ids := []string{
		"0",
		"3",
		"fa3-57",
		"fffff-ffffffff-ffa",
	}
	for _, str := range ids {
		id := ParseIDString(str)
		assert.NotEqual(t, BadId, id)
		str2 := id.String()
		assert.Equal(t, str, str2)
		fullstr := string(id.Hex583())
		id2 := ParseIDString(fullstr)
		assert.Equal(t, id, id2)
	}
}

func TestFieldNameType(t *testing.T) {
	src := uint64(0x8e)
	seq := uint64(0x82f0)
	id := IDFromSrcSeqOff(src, seq, ExampleName)
	assert.Equal(t, "8e-82f0-32", id.String())
}
