package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"io"
	"slices"
)

type Merger interface {
	// merges values, sorted old to new
	Merge(inputs [][]byte) []byte
}

type PebbleMergeAdaptor struct {
	id   rdx.ID
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
	res = rdx.Xmerge(a.rdt, inputs)
	return res, nil, nil
}
