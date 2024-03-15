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
	case 'A': // object's ref is immutable
		res = Amerge(inputs)
	case 'C':
		res = CMerge(inputs)
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
		res = NoMerge(inputs)
	}
	return res, nil, nil
}

func Amerge(inputs [][]byte) []byte {
	return inputs[0]
}

func NoMerge(inputs [][]byte) []byte {
	ret := make([]byte, 0, toytlv.TotalLen(inputs))
	for _, input := range inputs {
		ret = append(ret, input...)
	}
	return ret
}
