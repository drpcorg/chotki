package main

import (
	"github.com/cockroachdb/pebble"
)

const ExampleTypeId uint64 = (0x8e << SeqOffBits) | (0x5e84 << OffBits)
const ExampleName = (1 << FieldTypeBits) | ('S' - 'A')
const ExampleScore = (2 << FieldTypeBits) | ('C' - 'A')

type Example struct {
	Name  string
	Score int64
}

type LoaderStorer interface {
	// Read data from an iterator
	Load(i *pebble.Iterator) error
	// Compare to the stored state, serialize the changes
	Store(i *pebble.Iterator) (changes [][]byte, err error)
}

func (ex *Example) Load(i *pebble.Iterator) (err error) {
	if !i.Next() {
		return nil
	}
	if Parse583Off(i.Key()[1:]) == ExampleName {
		ex.Name = Splain(i.Value())
		if !i.Next() {
			return
		}
	}
	// todo skip garbage
	if Parse583Off(i.Key()[1:]) == ExampleScore {
		ex.Score = Iplain(i.Value())
		if !i.Next() {
			return
		}
	}
	return
}

func (x *Example) Store(i *pebble.Iterator) (changes [][]byte, err error) {
	if !i.Next() {
		return nil, nil
	}
	if Parse583Off(i.Key()[1:]) == ExampleName {
		new_tlv := Stlv(x.Name)
		delta := Sdelta(i.Value(), new_tlv)
		if delta != nil {
			changes = append(changes, delta)
		}
		if !i.Next() {
			return nil, nil
		}
	}
	if Parse583Off(i.Key()[1:]) == ExampleScore {
		new_tlv := Itlv(x.Score)
		delta := Idelta(i.Value(), new_tlv)
		if delta != nil {
			changes = append(changes, delta)
		}
		if !i.Next() {
			return
		}
	}
	return
}
