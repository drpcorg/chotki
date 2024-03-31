package rdx

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type SortedStringIterator struct {
	s string
}

func (ssi *SortedStringIterator) Next() bool {
	ssi.s = ssi.s[1:]
	return len(ssi.s) > 0
}
func (ssi *SortedStringIterator) Valid() bool {
	return len(ssi.s) > 0
}
func (ssi *SortedStringIterator) Merge(b SortedIterator) int {
	bb := b.(*SortedStringIterator)
	ac := ssi.s[0]
	bc := bb.s[0]
	if ac == bc {
		return MergeA
	} else if ac < bc {
		return MergeAB
	} else {
		return MergeBA
	}
}
func (ssi *SortedStringIterator) Value() []byte {
	return []byte(ssi.s[:1])
}

func TestItHeap_Pop(t *testing.T) {
	ih := ItHeap[*SortedStringIterator]{}
	ih.Push(&SortedStringIterator{"acdfi"})
	ih.Push(&SortedStringIterator{"abefh"})
	ih.Push(&SortedStringIterator{"bgjk"})
	var res []byte
	for ih.Len() > 0 {
		res = append(res, ih.Next()...)
	}
	assert.Equal(t, "abcdefghijk", string(res))
}
