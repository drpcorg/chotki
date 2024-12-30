package chotki

import (
	"io"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
)

type Merger interface {
	// merges values, sorted old to new
	Merge(inputs [][]byte) []byte
}

func PebbleAdaptorMerge(key, value []byte) (pebble.ValueMerger, error) {
	/*if len(key) != 10 {
		return nil, nil
	}*/
	id, rdt := OKeyIdRdt(key)
	pma := PebbleMergeAdaptor{
		id:   id,
		rdt:  rdt,
		vals: make([][]byte, 0, 16),
		bulk: make([]byte, 0, 4096),
	}
	pma.AddInput(value)
	return &pma, nil
}

type PebbleMergeAdaptor struct {
	id   rdx.ID
	rdt  byte
	old  bool
	vals [][]byte
	bulk []byte
}

func (a *PebbleMergeAdaptor) AddInput(input []byte) {
	l := len(a.bulk)
	a.bulk = append(a.bulk, input...)
	copy(a.bulk[l:], input)
	a.vals = append(a.vals, a.bulk[l:])
}

func (a *PebbleMergeAdaptor) MergeNewer(value []byte) error {
	a.AddInput(value)
	return nil
}
func (a *PebbleMergeAdaptor) MergeOlder(value []byte) error {
	a.AddInput(value)
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
	return res, a, nil
}

func (a *PebbleMergeAdaptor) Close() error {
	a.vals = nil
	a.bulk = nil
	return nil
}
