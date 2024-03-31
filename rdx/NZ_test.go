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
		toytlv.Record('U', ZipUint64Pair(3, 2)),
		toytlv.Record('U', ZipUint64Pair(3, 3)),
		toytlv.Record('U', ZipUint64Pair(4, 4)),
	)
	assert.Equal(t, uint64(10), Nnative(two))

	three := Nmerge([][]byte{one, two})

	correct := toytlv.Concat(
		toytlv.Record('U', ZipUint64Pair(1, 1)),
		toytlv.Record('U', ZipUint64Pair(3, 2)),
		toytlv.Record('U', ZipUint64Pair(3, 3)),
		toytlv.Record('U', ZipUint64Pair(4, 4)),
	)

	assert.Equal(t, correct, three)

}

func Itlv3(rev int64, src uint64, inc int64) []byte {
	return toytlv.Record('I',
		toytlv.TinyRecord('T', ZipIntUint64Pair(rev, src)),
		ZipInt64(inc),
	)
}

func TestZtlv(t *testing.T) {
	fact := Ztlv(123)
	correct := Itlv3(0, 0, 123)
	assert.Equal(t, correct, fact)
	str := Zstring(fact)
	assert.Equal(t, "123", str)
	inc := Zdelta(fact, 124)
	assert.Equal(t, int64(1), Znative(inc))
	// todo diff
}

func TestZmerge(t *testing.T) {
	one := toytlv.Concat(
		Itlv3(1, 1, 1),
		Itlv3(2, 2, 2),
		Itlv3(3, 3, 3),
	)
	assert.Equal(t, int64(6), Znative(one))
	two := toytlv.Concat(
		Itlv3(3, 2, 3),
		Itlv3(3, 3, 3),
		Itlv3(4, 4, 4),
	)
	assert.Equal(t, int64(10), Znative(two))

	three := Zmerge([][]byte{one, two})

	correct := toytlv.Concat(
		Itlv3(1, 1, 1),
		Itlv3(3, 2, 3),
		Itlv3(3, 3, 3),
		Itlv3(4, 4, 4),
	)
	assert.Equal(t, int64(11), Znative(correct))

	assert.Equal(t, correct, three)

}
