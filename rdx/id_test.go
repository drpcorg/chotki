package rdx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIDCreation(t *testing.T) {
	t.Run("NewID", func(t *testing.T) {
		id := NewID(1, 2, 3)
		assert.Equal(t, uint64(1), id.Src())
		assert.Equal(t, uint64(2), id.Seq())
		assert.Equal(t, uint64(3), id.Off())
	})

	t.Run("IDFromSrcPro", func(t *testing.T) {
		id := IDfromSrcPro(1, 2)
		assert.Equal(t, uint64(1), id.Src())
		assert.Equal(t, uint64(2), id.Pro())
	})

	t.Run("IDFromSrcSeqOff", func(t *testing.T) {
		id := IDFromSrcSeqOff(1, 2, 3)
		assert.Equal(t, uint64(1), id.Src())
		assert.Equal(t, uint64(2), id.Seq())
		assert.Equal(t, uint64(3), id.Off())
	})
}

func TestIDGetters(t *testing.T) {
	id := IDFromSrcSeqOff(1, 2, 3)

	t.Run("Src", func(t *testing.T) {
		assert.Equal(t, uint64(1), id.Src())
	})

	t.Run("Seq", func(t *testing.T) {
		assert.Equal(t, uint64(2), id.Seq())
	})

	t.Run("Off", func(t *testing.T) {
		assert.Equal(t, uint64(3), id.Off())
	})

	t.Run("Pro", func(t *testing.T) {
		assert.Equal(t, uint64(2<<offBits|3), id.Pro())
	})
}

func TestIDOperations(t *testing.T) {
	id := IDFromSrcSeqOff(1, 2, 3)

	t.Run("ProAnd", func(t *testing.T) {
		result := id.ProAnd(0x0F)
		assert.Equal(t, uint64(3), result.Pro())
	})

	t.Run("ProOr", func(t *testing.T) {
		result := id.ProOr(0xF0)
		assert.Equal(t, uint64(0xF0|id.Pro()), result.Pro())
	})

	t.Run("ProPlus", func(t *testing.T) {
		result := id.ProPlus(1)
		assert.Equal(t, id.Pro()+1, result.Pro())
	})

	t.Run("ProMinus", func(t *testing.T) {
		result := id.ProMinus(1)
		assert.Equal(t, id.Pro()-1, result.Pro())
	})

	t.Run("ZeroOff", func(t *testing.T) {
		result := id.ZeroOff()
		assert.Equal(t, uint64(0), result.Off())
		assert.Equal(t, id.Src(), result.Src())
		assert.Equal(t, id.Seq(), result.Seq())
	})

	t.Run("ToOff", func(t *testing.T) {
		result := id.ToOff(5)
		assert.Equal(t, uint64(5), result.Off())
		assert.Equal(t, id.Src(), result.Src())
		assert.Equal(t, id.Seq(), result.Seq())
	})

	t.Run("IncPro", func(t *testing.T) {
		result := id.IncPro(1)
		assert.Equal(t, id.Seq()+1, result.Seq())
		assert.Equal(t, id.Src(), result.Src())
	})
}

func TestIDComparison(t *testing.T) {
	t.Run("Less", func(t *testing.T) {
		id1 := IDFromSrcSeqOff(1, 1, 0)
		id2 := IDFromSrcSeqOff(1, 2, 0)
		id3 := IDFromSrcSeqOff(2, 1, 0)

		assert.True(t, id1.Less(id2))
		assert.True(t, id1.Less(id3))
		assert.True(t, id2.Less(id3))

		assert.False(t, id2.Less(id1))
		assert.False(t, id3.Less(id1))
		assert.False(t, id3.Less(id2))
	})
}

func TestIDSerialization(t *testing.T) {
	id := IDFromSrcSeqOff(0x8e, 0x82f0, 1)

	t.Run("Bytes", func(t *testing.T) {
		bytes := id.Bytes()
		restored := IDFromBytes(bytes)
		assert.Equal(t, id, restored)
	})

	t.Run("ZipBytes", func(t *testing.T) {
		zipped := id.ZipBytes()
		unzipped := IDFromZipBytes(zipped)
		assert.Equal(t, id, unzipped)
	})
}

func TestIDStringConversion(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		id := IDFromSrcSeqOff(0x8e, 0x82f0, 1)
		assert.Equal(t, "8e-82f0-1", id.String())
	})

	t.Run("IDFromString", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected ID
		}{
			{"0-0", ID0},
			{"8e-82f0-1", IDFromSrcSeqOff(0x8e, 0x82f0, 1)},
			{"1-2-3-4", BadId},
		}

		for _, tc := range testCases {
			t.Run(tc.input, func(t *testing.T) {
				result := IDFromString(tc.input)
				assert.Equal(t, tc.expected, result)
			})
		}
	})
}

func TestIDParsing(t *testing.T) {
	t.Run("readIDFromString", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected ID
			rest     string
		}{
			{"8e-82f0-1", IDFromSrcSeqOff(0x8e, 0x82f0, 1), ""},
			{"8e-82f0-1rest", IDFromSrcSeqOff(0x8e, 0x82f0, 1), "rest"},
		}

		for _, tc := range testCases {
			t.Run(tc.input, func(t *testing.T) {
				result, rest := readIDFromString([]byte(tc.input))
				assert.Equal(t, tc.expected, result)
				assert.Equal(t, tc.rest, string(rest))
			})
		}
	})
}
