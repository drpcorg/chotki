package rdx

import (
	"testing"

	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
)

func TestTLV(t *testing.T) {
	body := []byte("test")
	tlv := TlvFIRST(234, 123, body)
	time, src, val := ParseFIRST(tlv)
	assert.Equal(t, 234, int(time))
	assert.Equal(t, 123, int(src))
	assert.Equal(t, body, val)

	doc := TlvFIRST(4, 5, ZipInt64(-11))
	assert.Equal(t, []byte{0x32, 0x08, 0x05, 0x15}, doc)
}

func TestI(t *testing.T) {
	str1 := "123"
	tlv1 := Iparse(str1)
	int1 := int64(123)
	assert.Equal(t, tlv1, Itlv(int1))
	str2 := "345"
	tlv2 := Iparse(str2)
	int2 := Inative(tlv2)
	assert.Equal(t, int64(345), int2)
	delta12 := Idelta(tlv1, 345)
	merged := Imerge([][]byte{tlv1, delta12})
	assert.Equal(t, str2, Istring(merged))
}

func TestS(t *testing.T) {
	str1 := "fcuk\n\"zis\"\n"
	tlv1 := Stlv(str1)
	quoted := Sstring(tlv1)
	unquoted := string(Snative(Sparse(quoted)))
	assert.Equal(t, str1, unquoted)
	assert.Equal(t, str1, Snative(tlv1))
	str2 := "fcuk\n\"zat\"\n"
	delta12 := Sdelta(tlv1, str2)
	merged := Smerge([][]byte{tlv1, delta12})
	assert.Equal(t, str2, Snative(merged))
}

func TestR(t *testing.T) {
	str1 := "ae-32"
	tlv1 := Rparse(str1)
	id1 := IDFromSrcSeqOff(0xae, 0x32, 0)
	id2 := Rnative(tlv1)
	assert.Equal(t, id1, id2)

	str2 := "ae-33"
	tlv2 := Rparse(str2)
	delta12 := Rdelta(tlv1, Rnative(tlv2))
	merged := Rmerge([][]byte{tlv1, delta12})
	assert.Equal(t, str2, Rstring(merged))
}

func TestF(t *testing.T) {
	str1 := "3.1415"
	tlv1 := Fparse(str1)
	id1 := 3.1415
	id2 := Fnative(tlv1)
	assert.Equal(t, id1, id2)

	str2 := "3.141592"
	tlv2 := Fparse(str2)
	delta12 := Fdelta(tlv1, Fnative(tlv2))
	merged := Fmerge([][]byte{tlv1, delta12})
	assert.Equal(t, str2, Fstring(merged))
}

func TestIMerge(t *testing.T) {
	var i1 int64 = 123
	var i2 int64 = 345
	tlv1 := Itlv(i1)
	tlv2 := Idelta(tlv1, i2)
	merge := Imerge(toyqueue.Records{tlv1, tlv2})
	assert.Equal(t, tlv2, merge)
}

func TestLWWTie(t *testing.T) {
	a := TlvFIRST(4, 8, ZipInt64(1))
	b := TlvFIRST(4, 7, ZipInt64(2))
	c := TlvFIRST(4, 5, ZipInt64(2))
	d := Imerge(toyqueue.Records{a, b, c})
	assert.Equal(t, int64(2), Inative(d))
	rev, src, _ := ParseFIRST(d)
	assert.Equal(t, int64(4), rev)
	assert.Equal(t, uint64(7), src)
}
