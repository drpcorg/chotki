package rdx

import (
	"testing"

	tlv "github.com/drpcorg/chotki/protocol"
	"github.com/stretchr/testify/assert"
)

func TestNtlv(t *testing.T) {
	fact := Ntlv(123)
	correct := Ntlvt(123, 0)
	assert.Equal(t, correct, fact)
	str := Nstring(fact)
	assert.Equal(t, "123", str)
	clock := LocalLogicalClock{}
	inc := Ndelta(fact, 124, &clock)
	assert.Equal(t, uint64(124), Nnative(inc))
	// todo diff
}

func TestNmerge(t *testing.T) {
	one := tlv.Concat(
		Ntlvt(1, 1),
		Ntlvt(2, 2),
		Ntlvt(3, 3),
	)
	assert.Equal(t, uint64(6), Nnative(one))
	two := tlv.Concat(
		Ntlvt(3, 2),
		Ntlvt(3, 3),
		Ntlvt(4, 4),
	)
	assert.Equal(t, uint64(10), Nnative(two))

	three := Nmerge([][]byte{one, two})

	correct := tlv.Concat(
		Ntlvt(1, 1),
		Ntlvt(3, 2),
		Ntlvt(3, 3),
		Ntlvt(4, 4),
	)

	assert.Equal(t, correct, three)

}

func TestZtlv(t *testing.T) {
	fact := Ztlv(123)
	correct := Itlve(0, 0, 123)
	assert.Equal(t, correct, fact)
	str := Zstring(fact)
	assert.Equal(t, "123", str)
	clock := LocalLogicalClock{Source: 1}
	inc := Zdelta(fact, 124, &clock)
	assert.Equal(t, int64(1), Znative(inc))
	// todo diff
}

func TestZmerge(t *testing.T) {
	one := tlv.Concat(
		Itlve(1, 1, 1),
		Itlve(2, 2, 2),
		Itlve(3, 3, 3),
	)
	assert.Equal(t, int64(6), Znative(one))
	two := tlv.Concat(
		Itlve(3, 2, 3),
		Itlve(3, 3, 3),
		Itlve(4, 4, 4),
	)
	assert.Equal(t, int64(10), Znative(two))

	three := Zmerge([][]byte{one, two})

	correct := tlv.Concat(
		Itlve(1, 1, 1),
		Itlve(3, 2, 3),
		Itlve(3, 3, 3),
		Itlve(4, 4, 4),
	)
	assert.Equal(t, int64(11), Znative(correct))

	assert.Equal(t, correct, three)

}
