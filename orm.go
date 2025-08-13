package chotki

import (
	"context"
	"iter"
	"reflect"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/chotki_errors"
	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

type NativeObject interface {
	// Read data from an iterator
	Load(field uint64, rdt byte, tlv []byte) error
	// Compare to the stored state, serialize the changes
	Store(field uint64, rdt byte, old []byte, clock rdx.Clock) (changes []byte, err error)
}

type ORM struct {
	Host    *Chotki
	Snap    *pebble.Snapshot
	objects *sync.Map
	ids     sync.Map // xsync sometimes does not correctly work with pointers
	lock    sync.Mutex
}

func NewORM(host *Chotki, snap *pebble.Snapshot) *ORM {
	return &ORM{
		Host: host,
		Snap: snap,

		objects: &sync.Map{},
	}
}

// New object of the same type get persisted and registered with the ORM
func (orm *ORM) New(ctx context.Context, cid rdx.ID, objs ...NativeObject) (err error) {
	fields, e := orm.Host.ClassFields(cid)
	if e != nil {
		return e
	}
	for _, obj := range objs {
		tlv := protocol.Records{}
		for i := 1; i < len(fields) && err == nil; i++ {
			field := &fields[i]
			var edit []byte
			edit, err = obj.Store(uint64(i), field.RdxType, nil, orm.Host.Clock())
			tlv = append(tlv, protocol.Record(field.RdxType, edit))
		}
		var id rdx.ID
		id, err = orm.Host.CommitPacket(ctx, 'O', cid, tlv)
		if err != nil {
			return err
		}
		orm.ids.Store(obj, id)
		orm.objects.Store(id, obj)
	}
	return nil
}

// Save the registered object's changes.
// Much faster than SaveALl() esp if you loaded many, modified few.
func (orm *ORM) Save(ctx context.Context, objs ...NativeObject) (err error) {
	for _, obj := range objs {
		id := orm.FindID(obj)
		if id == rdx.BadId {
			return chotki_errors.ErrObjectUnknown
		}
		it := orm.Host.ObjectIterator(id, orm.Snap)
		if it == nil {
			err = chotki_errors.ErrObjectUnknown
			break
		}
		cid := rdx.IDFromZipBytes(it.Value())
		fields, e := orm.Host.ClassFields(cid)
		if e != nil {
			_ = it.Close()
			return e
		}
		var changes protocol.Records
		flags := [64]bool{}
		for it.Next() {
			lid, rdt := host.OKeyIdRdt(it.Key())
			off := lid.Off()
			change, e := obj.Store(off, rdt, it.Value(), orm.Host.Clock())
			flags[off] = true
			if e != nil { // the db may have garbage
				_ = 0 // todo
			}
			if len(change) > 0 {
				changes = append(changes, protocol.Record('F', rdx.ZipUint64(off)))
				changes = append(changes, protocol.Record(rdt, change))
			}
		}
		for off := 1; off < len(fields); off++ {
			if flags[off] {
				continue
			}
			change, _ := obj.Store(uint64(off), fields[off].RdxType, nil, orm.Host.Clock())
			if change != nil {
				changes = append(changes, protocol.Record('F', rdx.ZipUint64(uint64(off))))
				changes = append(changes, protocol.Record(fields[off].RdxType, change))
			}
		}
		_ = it.Close()
		if len(changes) != 0 {
			_, err = orm.Host.CommitPacket(ctx, 'E', id, changes)
		}
	}
	return err
}

// SaveAll the changed fields; this will scan the objects and
// their database records.
func (orm *ORM) SaveAll(ctx context.Context) (err error) {
	orm.objects.Range(func(_ any, obj any) bool {
		err = orm.Save(ctx, obj.(NativeObject))
		return err == nil
	})

	return
}

// Clear forgets all the objects loaded; all the unsaved changes discarded
func (orm *ORM) Clear() error {
	orm.lock.Lock()
	defer orm.lock.Unlock()

	if orm.Host == nil {
		return chotki_errors.ErrClosed
	}
	orm.objects.Clear()
	if orm.Snap != nil {
		_ = orm.Snap.Close()
	}
	orm.Snap = orm.Host.Database().NewSnapshot()
	return nil
}

func (orm *ORM) Close() error {
	orm.lock.Lock()
	defer orm.lock.Unlock()

	if orm.Host == nil {
		return chotki_errors.ErrClosed
	}
	orm.objects.Clear()
	orm.ids = sync.Map{}
	orm.Host = nil
	_ = orm.Snap.Close()
	orm.Snap = nil
	return nil
}

func (orm *ORM) UpdateObject(obj NativeObject, snap *pebble.Snapshot) error {
	id := orm.FindID(obj)
	if id == rdx.BadId {
		return chotki_errors.ErrObjectUnknown
	}
	it := orm.Host.ObjectIterator(id, snap)
	if it == nil {
		return chotki_errors.ErrObjectUnknown
	}
	seq := orm.Snap.Seq()
	for it.Next() {
		lid, rdt := host.OKeyIdRdt(it.Key())
		off := lid.Off()
		if it.Seq() > seq {
			e := obj.Load(off, rdt, it.Value())
			// todo as of now, this may overwrite the object's changes
			if e != nil { // the db may have garbage
				_ = 0 // todo
			}
		}
	}
	_ = it.Close()
	return nil
}

// UpdateAll the registered objects to the new db state
func (orm *ORM) UpdateAll() (err error) {
	snap := orm.Host.Database().NewSnapshot()
	orm.objects.Range(func(_ any, obj any) bool {
		err = orm.UpdateObject(obj.(NativeObject), snap)
		return err == nil
	})
	orm.lock.Lock()
	if orm.Snap != nil {
		_ = orm.Snap.Close()
	}
	orm.Snap = snap
	orm.lock.Unlock()
	return
}

// Saves all the changes, updates all the objects to the current db state.
func (orm *ORM) SyncAll(ctx context.Context) (err error) {
	err = orm.SaveAll(ctx)
	if err == nil {
		err = orm.UpdateAll()
	}
	return
}

// Load the object's state from the db, register the object.
// If an object is already registered for that id, returns the old one.
// The new one is not used then.
func (orm *ORM) Load(id rdx.ID, blanc NativeObject, skipFields ...uint64) (obj NativeObject, err error) {
	pre, ok := orm.objects.Load(id)
	if ok {
		return pre.(NativeObject), nil
	}
	fro, til := host.ObjectKeyRange(id)
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: til,
	}
	it := orm.Snap.NewIter(&io)
	for it.SeekGE(fro); it.Valid(); it.Next() {
		lid, rdt := host.OKeyIdRdt(it.Key())
		off := lid.Off()
		if !slices.Contains(skipFields, off) {
			e := blanc.Load(off, rdt, it.Value())
			if e != nil { // the db may have garbage
				_ = 0 // todo
			}
		}
	}
	_ = it.Close()
	pre, ok = orm.objects.Load(id)
	if ok {
		return pre.(NativeObject), nil
	}
	orm.objects.Store(id, blanc)
	orm.ids.Store(blanc, id)
	return blanc, nil
}

// Use full scan index to find all objects of a class.
func SeekClass[T NativeObject](orm *ORM, cid rdx.ID) iter.Seq[T] {
	return func(yield func(obj T) bool) {
		for id := range orm.Host.IndexManager.SeekClass(cid, orm.Snap) {
			var obj T
			if reflect.TypeOf(obj).Kind() == reflect.Ptr {
				obj = reflect.New(reflect.TypeOf(obj).Elem()).Interface().(T)
			}
			obk, err := orm.Load(id, obj)
			if err != nil {
				return
			}
			if !yield(obk.(T)) {
				return
			}
		}
	}
}

// Use hash index to find an object by its hash.
func GetByHash[T NativeObject](orm *ORM, cid rdx.ID, fid uint32, tlv []byte) (T, error) {
	var obj T
	id, err := orm.Host.IndexManager.GetByHash(cid, fid, tlv, orm.Snap)
	if err != nil {
		return obj, err
	}
	if reflect.TypeOf(obj).Kind() == reflect.Ptr {
		obj = reflect.New(reflect.TypeOf(obj).Elem()).Interface().(T)
	}
	obk, err := orm.Load(id, obj)
	if err != nil {
		return obj, err
	}
	return obk.(T), nil
}

// Use hash index to find an object rdx.ID by its hash.
func (orm *ORM) GetIdByHash(cid rdx.ID, fid uint32, tlv []byte) (rdx.ID, error) {
	return orm.Host.IndexManager.GetByHash(cid, fid, tlv, orm.Snap)
}

// Use full scan index to find all object rdx.IDs of a class.
func (orm *ORM) SeekIds(cid rdx.ID) iter.Seq[rdx.ID] {
	return orm.Host.IndexManager.SeekClass(cid, orm.Snap)
}

// Find a registered object given its id. nil if none.
func (orm *ORM) Object(id rdx.ID) NativeObject {
	objV, _ := orm.objects.Load(id)
	return objV.(NativeObject)
}

// Find the ID of the registered object's.
func (orm *ORM) FindID(obj NativeObject) rdx.ID {
	id, ok := orm.ids.Load(obj)
	if !ok {
		id = rdx.BadId
	}
	return id.(rdx.ID)
}
