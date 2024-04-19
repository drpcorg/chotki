package examples

import (
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/toytlv"
)

const ExampleName = 1
const ExampleScore = 2

type Example struct {
	Name  string
	Score int64
}

func (ex *Example) Load(i *pebble.Iterator) (err error) {
	if !i.Next() {
		return nil
	}
	id, _ := chotki.OKeyIdRdt(i.Key())
	if id.Off() == ExampleName {
		ex.Name = rdx.Snative(i.Value())
		if !i.Next() {
			return
		}
		id, _ = chotki.OKeyIdRdt(i.Key())
	}
	// todo skip garbage
	if id.Off() == ExampleScore {
		ex.Score = rdx.Inative(i.Value())
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
	id, rdt := chotki.OKeyIdRdt(i.Key()) // fixme skip garbage
	if id.Off() == ExampleName && rdt == 'S' {
		delta := rdx.Sdelta(i.Value(), x.Name)
		if delta != nil {
			changes = append(changes, toytlv.Record('F', rdx.ZipUint64(ExampleName)))
			changes = append(changes, toytlv.Record('S', delta))
		}
		if !i.Next() {
			return nil, nil
		}
		id, rdt = chotki.OKeyIdRdt(i.Key()) // todo good iter
	}
	if id.Off() == ExampleScore && rdt == 'I' {
		delta := rdx.Idelta(i.Value(), x.Score)
		if delta != nil {
			changes = append(changes, toytlv.Record('F', rdx.ZipUint64(ExampleScore)))
			changes = append(changes, toytlv.Record('I', delta))
		}
		if !i.Next() {
			return
		}
	}
	return
}
