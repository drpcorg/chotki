package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeVarPair(t *testing.T) {
	nums := []uint64{
		0xca,
		0xbeff,
		0x12345678,
		0x7777777788888888,
	}
	for i := 0; i < len(nums); i++ {
		for j := 0; j < len(nums); j++ {
			one := nums[i]
			two := nums[j]
			bin := ZipUint64Pair(one, two)
			einz, twei := UnzipUint64Pair(bin)
			assert.Equal(t, one, einz)
			assert.Equal(t, two, twei)
		}
	}
}

func TestZigZagInt64(t *testing.T) {
	test := map[int64]uint64{
		0:   0,
		-14: 27,
		-10: 19,
		7:   14,
		20:  40,
	}
	for i, u := range test {
		u2 := ZigZagInt64(i)
		assert.Equal(t, u, u2)
		i2 := ZagZigUint64(u2)
		assert.Equal(t, i, i2)
	}
}

func TestZipFloat64(t *testing.T) {
	test := map[float64]int{
		0:     0,
		1:     2,
		1234:  3,
		12.25: 3,
	}
	for f, l := range test {
		u := ZipFloat64(f)
		assert.Equal(t, l, len(u))
		f2 := UnzipFloat64(u)
		assert.Equal(t, f, f2)
	}
}

func TestZipID(t *testing.T) {
	var i0 int64 = -7
	var u0 uint64 = 15
	zip := ZipIntUint64Pair(i0, u0)
	i, u := UnzipIntUint64Pair(zip)
	assert.Equal(t, i0, i)
	assert.Equal(t, u0, u)
}
