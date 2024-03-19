package main

import (
	"github.com/learn-decentralized-systems/toytlv"
	"io"
	"slices"
)

type Merger interface {
	// merges values, sorted old to new
	Merge(inputs [][]byte) []byte
}

type PebbleMergeAdaptor struct {
	id   id64
	rdt  byte
	old  bool
	vals [][]byte
}

func (a *PebbleMergeAdaptor) MergeNewer(value []byte) error {
	a.vals = append(a.vals, value)
	return nil
}
func (a *PebbleMergeAdaptor) MergeOlder(value []byte) error {
	a.vals = append(a.vals, value)
	a.old = true
	return nil
}

func (a *PebbleMergeAdaptor) Finish(includesBase bool) (res []byte, cl io.Closer, err error) {
	if a.old {
		slices.Reverse(a.vals)
	}
	inputs := a.vals
	if len(inputs) == 0 {
		return nil, nil, nil
	}
	switch a.rdt {
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
