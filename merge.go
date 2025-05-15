package chotki

import (
	"io"
	"slices"

	"github.com/drpcorg/chotki/rdx"
)

type Merger interface {
	// merges values, sorted old to new
	Merge(inputs [][]byte) []byte
}

type PebbleMergeAdaptor struct {
	rdt  byte
	old  bool
	vals [][]byte
}

func (a *PebbleMergeAdaptor) MergeNewer(value []byte) error {
	target := make([]byte, len(value))
	copy(target, value)
	a.vals = append(a.vals, target)
	return nil
}
func (a *PebbleMergeAdaptor) MergeOlder(value []byte) error {
	target := make([]byte, len(value))
	copy(target, value)
	a.vals = append(a.vals, target)
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
	res = rdx.Xmerge(a.rdt, inputs)
	return res, nil, nil
}
