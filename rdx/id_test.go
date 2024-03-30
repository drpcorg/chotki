package rdx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCases(t *testing.T) {
	off := IDFromString("0ff")
	assert.Equal(t, uint64(off), uint64(0xff))

	zip := ID(1).ZipBytes()
	assert.Equal(t, ID(1), IDFromZipBytes(zip))
}

func TestParseID(t *testing.T) {
	ids := []string{
		"0",
		"3",
		"fa3-57",
		"fffff-ffffffff-ffa",
	}
	for _, str := range ids {
		id := IDFromString(str)
		assert.NotEqual(t, BadId, id)
		str2 := id.String()
		assert.Equal(t, str, str2)
		fullstr := string(id.Hex583())
		id2 := IDFromString(fullstr)
		assert.Equal(t, id, id2)
	}
}

func TestFieldNameType(t *testing.T) {
	src := uint64(0x8e)
	seq := uint64(0x82f0)
	id := IDFromSrcSeqOff(src, seq, 1)
	assert.Equal(t, "8e-82f0-1", id.String())
}
