package rdx

import (
	"github.com/drpcorg/chotki/toytlv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNtlv(t *testing.T) {
	fact := Ntlv(123)
	correct := toytlv.Record(Term, ZipUint64Pair(123, 0))
	assert.Equal(t, correct, fact)
	str := Nstring(fact)
	assert.Equal(t, "123", str)
	inc := Ndelta(fact, 124)
	assert.Equal(t, uint64(1), Nnative(inc))
	// todo diff
}

func TestNmerge(t *testing.T) {
	one := toytlv.Concat(
		toytlv.Record(Term, ZipUint64Pair(1, 1)),
		toytlv.Record(Term, ZipUint64Pair(2, 2)),
		toytlv.Record(Term, ZipUint64Pair(3, 3)),
	)
	assert.Equal(t, uint64(6), Nnative(one))
	two := toytlv.Concat(
		toytlv.Record(Term, ZipUint64Pair(3, 2)),
		toytlv.Record(Term, ZipUint64Pair(3, 3)),
		toytlv.Record(Term, ZipUint64Pair(4, 4)),
	)
	assert.Equal(t, uint64(10), Nnative(two))

	three := Nmerge([][]byte{one, two})

	correct := toytlv.Concat(
		toytlv.Record(Term, ZipUint64Pair(1, 1)),
		toytlv.Record(Term, ZipUint64Pair(3, 2)),
		toytlv.Record(Term, ZipUint64Pair(3, 3)),
		toytlv.Record(Term, ZipUint64Pair(4, 4)),
	)

	assert.Equal(t, correct, three)

}

func TestZtlv(t *testing.T) {
	fact := Ztlv(123)
	correct := Itlve(0, 0, 123)
	assert.Equal(t, correct, fact)
	str := Zstring(fact)
	assert.Equal(t, "123", str)
	inc := Zdelta(fact, 124)
	assert.Equal(t, int64(1), Znative(inc))
	// todo diff
}

func TestZmerge(t *testing.T) {
	one := toytlv.Concat(
		Itlve(1, 1, 1),
		Itlve(2, 2, 2),
		Itlve(3, 3, 3),
	)
	assert.Equal(t, int64(6), Znative(one))
	two := toytlv.Concat(
		Itlve(3, 2, 3),
		Itlve(3, 3, 3),
		Itlve(4, 4, 4),
	)
	assert.Equal(t, int64(10), Znative(two))

	three := Zmerge([][]byte{one, two})

	correct := toytlv.Concat(
		Itlve(1, 1, 1),
		Itlve(3, 2, 3),
		Itlve(3, 3, 3),
		Itlve(4, 4, 4),
	)
	assert.Equal(t, int64(11), Znative(correct))

	assert.Equal(t, correct, three)

}
