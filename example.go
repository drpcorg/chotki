package main

import (
	"github.com/cockroachdb/pebble"
)

const ExampleTypeId uint64 = (0x8e << SeqOffBits) | (0x5e84 << OffBits)
const ExampleName = (1 << FieldTypeBits) | ('S' - 'A')
const ExampleScore = (2 << FieldTypeBits) | ('C' - 'A')

type Example struct {
	Name  String
	Score Counter
}

func (ex *Example) Apply(i *pebble.Iterator) error {
	if !i.Next() {
		return nil
	}
	if Parse583Off(i.Key()[1:]) == ExampleName {
		ex.Name.Apply(i.Value())
		if !i.Next() {
			return nil
		}
	}
	if Parse583Off(i.Key()[1:]) == ExampleScore {
		ex.Score.Apply(i.Value())
	}
	return nil
}

func (x *Example) Diff(id ID, base *pebble.Iterator, batch *pebble.Batch) error {
	changes := []byte{}
	if !base.Next() {
		return nil
	}
	if Parse583Off(base.Key()[1:]) == ExampleName {
		changes = append(changes, x.Name.Diff(id, base.Value())...)
		if !base.Next() {
			return nil
		}
	}
	if Parse583Off(base.Key()[1:]) == ExampleScore {
		changes = append(changes, x.Score.Diff(id, base.Value())...)
	}
	return nil
}
