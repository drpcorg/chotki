package rdx

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type SortedStringIterator struct {
	c byte
	s string
}

func (ssi *SortedStringIterator) Next() bool {
	if len(ssi.s) == 0 {
		return false
	}
	ssi.c = ssi.s[0]
	ssi.s = ssi.s[1:]
	return true
}
func (ssi *SortedStringIterator) Merge(b SortedIterator) int {
	bb := b.(*SortedStringIterator)
	ac := ssi.c
	bc := bb.c
	if ac == bc {
		return MergeA
	} else if ac < bc {
		return MergeAB
	} else {
		return MergeBA
	}
}
func (ssi *SortedStringIterator) Value() []byte {
	var ret = [1]byte{ssi.c}
	return ret[:]
}

func TestItHeap_Pop(t *testing.T) {
	ih := ItHeap[*SortedStringIterator]{}
	ih.Push(&SortedStringIterator{s: "abcdfi"})
	ih.Push(&SortedStringIterator{s: "abefh"})
	ih.Push(&SortedStringIterator{s: "bgjk"})
	var res []byte
	for ih.Len() > 0 {
		res = append(res, ih.Next()...)
	}
	assert.Equal(t, "abcdefghijk", string(res))
}
