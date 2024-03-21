package main

import (
	"github.com/cockroachdb/pebble"
	"github.com/learn-decentralized-systems/toytlv"
)

const ExampleName = 1
const ExampleScore = 2

type Example struct {
	Name  string
	Score int64
}

type StoreLoader interface {
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
		ex.Name = Snative(i.Value())
		if !i.Next() {
			return
		}
		id, rdt = OKeyIdRdt(i.Key())
	}
	// todo skip garbage
	if id.Off() == ExampleScore {
		ex.Score = Inative(i.Value())
		if !i.Next() {
			return
		}
	}
	return
}

func (x *Example) Store(i *pebble.Iterator) (changes [][]byte, err error) {
	if !i.Next() { // todo check type
		return nil, nil
	}
	id, rdt := OKeyIdRdt(i.Key()) // fixme skip garbage
	if id.Off() == ExampleName && rdt == 'S' {
		delta := Sdelta(i.Value(), x.Name)
		if delta != nil {
			changes = append(changes, toytlv.Record('F', ZipUint64(ExampleName)))
			changes = append(changes, toytlv.Record('S', delta))
		}
		if !i.Next() {
			return nil, nil
		}
		id, rdt = OKeyIdRdt(i.Key()) // todo good iter
	}
	if id.Off() == ExampleScore && rdt == 'I' {
		delta := Idelta(i.Value(), x.Score)
		if delta != nil {
			changes = append(changes, toytlv.Record('F', ZipUint64(ExampleScore)))
			changes = append(changes, toytlv.Record('I', delta))
		}
		if !i.Next() {
			return
		}
	}
	return
}
