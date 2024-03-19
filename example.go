package main

import (
	"github.com/cockroachdb/pebble"
)

const ExampleName = 1
const ExampleScore = 2

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
	id, rdt := OKeyIdRdt(i.Key())
	_ = rdt // fixme skip garbage
	if id.Off() == ExampleName {
		ex.Name = Splain(i.Value())
		if !i.Next() {
			return
		}
		id, rdt = OKeyIdRdt(i.Key())
	}
	// todo skip garbage
	if id.Off() == ExampleScore {
		ex.Score = Iplain(i.Value())
		if !i.Next() {
			return
		}
	}
	return
}

func (x *Example) Store(i *pebble.Iterator, src uint64) (changes [][]byte, err error) {
	if !i.Next() {
		return nil, nil
	}
	if Parse583Off(i.Key()[1:]) == ExampleName {
		delta := Sdelta(i.Value(), x.Name)
		if delta != nil {
			changes = append(changes, delta)
		}
		if !i.Next() {
			return nil, nil
		}
	}
	if Parse583Off(i.Key()[1:]) == ExampleScore {
		delta := Idelta(i.Value(), x.Score)
		if delta != nil {
			changes = append(changes, delta)
		}
		if !i.Next() {
			return
		}
	}
	return
}
