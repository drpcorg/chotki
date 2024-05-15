package rdx

import "github.com/drpcorg/chotki/protocol"

// append-only collection of Terms
type LogT []string

func Amerge(tlvs [][]byte) (merged []byte) {
	ih := ItHeap[*AIterator]{}
	for _, tlv := range tlvs {
		ih.Push(&AIterator{FIRSTIterator{TLV: tlv}})
	}
	for ih.Len() > 0 {
		merged = append(merged, ih.Next()...)
	}
	return
}

func Atlv(log LogT) (tlv []byte) {
	for n, t := range log {
		bare := FIRSTtlv(int64(n), 0, []byte(t))
		tlv = protocol.AppendHeader(tlv, Term, len(bare))
		tlv = append(tlv, bare...)
	}
	return
}

func AnativeT(tlv []byte) (log LogT) {
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
		if it.lit != Term {
			continue
		}
		log = append(log, string(it.val))
	}
	return
}

type AIterator struct {
	FIRSTIterator
}

func (a *AIterator) Merge(bb SortedIterator) int {
	b := bb.(*AIterator)
	if a.revz > b.revz {
		return MergeBA
	} else if a.revz < b.revz {
		return MergeAB
	}
	cmp := FIRSTcompare(a.one, b.one)
	if cmp == 0 {
		return MergeA
	} else if cmp < 0 {
		return MergeAB
	} else {
		return MergeBA
	}
}
