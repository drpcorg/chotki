package main

import (
	"github.com/learn-decentralized-systems/toytlv"
	"io"
)

type Merger interface {
	// merges values, sorted old to new
	Merge(inputs [][]byte) []byte
}

type PebbleMergeAdaptor struct {
	key        []byte
	inew, iold [][]byte
}

func (a *PebbleMergeAdaptor) MergeNewer(value []byte) error {
	a.inew = append(a.inew, value)
	return nil
}
func (a *PebbleMergeAdaptor) MergeOlder(value []byte) error {
	a.iold = append(a.iold, value)
	return nil
}

func (a *PebbleMergeAdaptor) Finish(includesBase bool) (res []byte, cl io.Closer, err error) {
	for i, j := 0, len(a.iold)-1; i < j; i, j = i+1, j-1 {
		a.iold[i], a.iold[j] = a.iold[j], a.iold[i]
	}
	inputs := append(a.iold, a.inew...)
	if len(inputs) == 0 {
		return nil, nil, nil
	}
	off := Parse583Off(a.key[1:])
	_, rdt := FieldNameType(off)
	switch rdt {
	case 'C':
		res = CMerge(inputs) // TODO error?
	case 'I':
		res = Imerge(inputs)
	case 'S':
		res = Smerge(inputs)
	case 'R':
		res = Rmerge(inputs)
	case 'F':
		res = Fmerge(inputs)
	case 'V':
		res = Vmerge(inputs)
	default:
		res = AMerge(inputs)
	}
	return res, nil, nil
}

func AMerge(inputs [][]byte) []byte {
	ret := make([]byte, 0, toytlv.TotalLen(inputs))
	for _, input := range inputs {
		ret = append(ret, input...)
	}
	return ret
}

func Vmerge(inputs [][]byte) (merged []byte) {
	var _heap [32]uint64
	heap := Uint64Heap(_heap[0:0])
	for i, in := range inputs { // fixme 4096
		id := ProbeID('V', in)
		reid := MakeID(id.Src(), id.Seq(), uint16(i)) // todo i order
		heap.Push(^uint64(reid))
	}
	prev := uint32(0)
	for len(heap) > 0 {
		id := ID(^heap.Pop())
		i := int(id.Off())
		_, hl, bl := toytlv.ProbeHeader(inputs[i])
		if id.Src() != prev {
			merged = append(merged, inputs[i][:hl+bl]...)
			prev = id.Src()
		}
		inputs[i] = inputs[i][hl+bl:]
		if len(inputs[i]) != 0 {
			id := ProbeID('V', inputs[i])
			reid := MakeID(id.Src(), id.Seq(), uint16(i)) // todo i order
			heap.Push(^uint64(reid))
		}
	}
	return

}
