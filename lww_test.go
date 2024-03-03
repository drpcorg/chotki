package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestI(t *testing.T) {
	str1 := "123"
	tlv1 := Iparse(str1)
	int1 := int64(123)
	assert.Equal(t, tlv1, Itlv(int1))
	str2 := "345"
	tlv2 := Iparse(str2)
	delta12 := Idelta(tlv1, tlv2)
	merged := Imerge([][]byte{tlv1, delta12})
	assert.Equal(t, str2, Istring(merged))
}

func TestS(t *testing.T) {
	str1 := "fcuk\n\"zis\"\n"
	tlv1 := Stlv(str1)
	quoted := Sstring(tlv1)
	unquoted := string(LWWvalue(Sparse(quoted)))
	assert.Equal(t, str1, unquoted)
	assert.Equal(t, str1, Splain(tlv1))
	str2 := "fcuk\n\"zat\"\n"
	tlv2 := Stlv(str2)
	delta12 := Sdelta(tlv1, tlv2)
	merged := Smerge([][]byte{tlv1, delta12})
	assert.Equal(t, str2, Splain(merged))
}

func TestR(t *testing.T) {
	str1 := "ae-32"
	tlv1 := Rparse(str1)
	id1 := MakeID(0xae, 0x32, 0)
	id2 := Rplain(tlv1)
	assert.Equal(t, id1, id2)

	str2 := "ae-33"
	tlv2 := Rparse(str2)
	delta12 := Rdelta(tlv1, tlv2)
	merged := Rmerge([][]byte{tlv1, delta12})
	assert.Equal(t, str2, Rstring(merged))
}

func TestF(t *testing.T) {
	str1 := "3.1415"
	tlv1 := Fparse(str1)
	id1 := 3.1415
	id2 := Fplain(tlv1)
	assert.Equal(t, id1, id2)

	str2 := "3.141592"
	tlv2 := Fparse(str2)
	delta12 := Fdelta(tlv1, tlv2)
	merged := Fmerge([][]byte{tlv1, delta12})
	assert.Equal(t, str2, Fstring(merged))
}
