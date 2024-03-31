package rdx

import (
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNtlv(t *testing.T) {
	fact := Ntlv(123)
	correct := toytlv.Record('U', ZipUint64Pair(123, 0))
	assert.Equal(t, correct, fact)
	str := Nstring(fact)
	assert.Equal(t, "123", str)
	inc := Ndelta(fact, 124)
	assert.Equal(t, uint64(1), Nnative(inc))
	// todo diff
}

func TestNmerge(t *testing.T) {
	one := toytlv.Concat(
		toytlv.Record('U', ZipUint64Pair(1, 1)),
		toytlv.Record('U', ZipUint64Pair(2, 2)),
		toytlv.Record('U', ZipUint64Pair(3, 3)),
	)
	assert.Equal(t, uint64(6), Nnative(one))
	two := toytlv.Concat(
		toytlv.Record('U', ZipUint64Pair(2, 2)),
		toytlv.Record('U', ZipUint64Pair(3, 3)),
		toytlv.Record('U', ZipUint64Pair(4, 4)),
	)
	assert.Equal(t, uint64(9), Nnative(two))

	three := Nmerge([][]byte{one, two})

	correct := toytlv.Concat(
		toytlv.Record('U', ZipUint64Pair(1, 1)),
		toytlv.Record('U', ZipUint64Pair(2, 2)),
		toytlv.Record('U', ZipUint64Pair(3, 3)),
		toytlv.Record('U', ZipUint64Pair(4, 4)),
	)

	assert.Equal(t, correct, three)

}
