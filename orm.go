package chotki

import (
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
	"sync"
)

type StoreLoader interface {
	// Read data from an iterator
	Load(i *pebble.Iterator) error
	// Compare to the stored state, serialize the changes
	Store(i *pebble.Iterator) (changes [][]byte, err error)
}

type ORM struct {
	Snap    *pebble.Snapshot
	objects map[rdx.ID]StoreLoader
	lock    sync.Mutex
}

func (orm *ORM) LoadObject(id rdx.ID, blanc StoreLoader) (obj StoreLoader, err error) {
	io := pebble.IterOptions{
		LowerBound: []byte{},
		UpperBound: []byte{},
	}
	it := orm.Snap.NewIter(&io)
	orm.lock.Lock()
	err = blanc.Load(it)
	if err == nil {
		orm.objects[id] = blanc
	}
	orm.lock.Unlock()
	_ = it.Close()
	return
}

func (orm *ORM) Object(id rdx.ID) (obj StoreLoader) {
	orm.lock.Lock()
	obj = orm.objects[id]
	orm.lock.Unlock()
	return // todo
}
