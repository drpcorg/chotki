package chotki

import (
	"bytes"
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"html/template"
	"sync"
)

type StoreLoader interface {
	// Read data from an iterator
	Load(i *pebble.Iterator) error
	// Compare to the stored state, serialize the changes
	Store(i *pebble.Iterator) (changes [][]byte, err error)
}

type NativeObject interface {
	// Read data from an iterator
	Load(field uint64, rdt byte, tlv []byte) error
	// Compare to the stored state, serialize the changes
	Store(field uint64, rdt byte, old []byte) (changes []byte, err error)
}

type ORM struct {
	Host    *Chotki
	Snap    *pebble.Snapshot
	objects map[rdx.ID]NativeObject
	ids     map[NativeObject]rdx.ID
	lock    sync.Mutex
}

// Save the object changes.
// Recommended, especially if you loaded many, modified few.
func (orm *ORM) Save(objs ...NativeObject) (err error) {
	for _, obj := range objs {
		id := orm.FindID(obj)
		if id == rdx.BadId {
			return ErrObjectUnknown
		}
		var it *pebble.Iterator
		it, err = ObjectIterator(id, orm.Snap)
		if err != nil {
			break
		}
		cid := rdx.IDFromZipBytes(it.Value())
		fields, e := orm.Host.ClassFields(cid)
		if e != nil {
			return e
		}
		var changes toyqueue.Records
		flags := [64]bool{}
		for it.Next() {
			lid, rdt := OKeyIdRdt(it.Key())
			off := lid.Off()
			change, e := obj.Store(off, rdt, it.Value())
			flags[off] = true
			if e != nil { // the db may have garbage
				_ = 0 // todo
			}
			if len(change) > 0 {
				changes = append(changes, toytlv.Record('F', rdx.ZipUint64(off)))
				changes = append(changes, toytlv.Record(rdt, change))
			}
		}
		for off := 1; off < len(fields); off++ {
			if flags[off] {
				continue
			}
			change, _ := obj.Store(uint64(off), fields[off].RdxType, nil)
			if change != nil {
				changes = append(changes, toytlv.Record('F', rdx.ZipUint64(uint64(off))))
				changes = append(changes, toytlv.Record(fields[off].RdxType, change))
			}
		}
		_ = it.Close()
		if len(changes) != 0 {
			_, err = orm.Host.CommitPacket('E', id, changes)
		}
	}
	return err
}

// SaveAll the changed fields; this will re-scan the objects in the database.
func (orm *ORM) SaveAll() (err error) {
	orm.lock.Lock()
	for _, obj := range orm.objects {
		err = orm.Save(obj)
		if err != nil {
			break
		}
	}
	orm.lock.Unlock()
	return
}

// Clear forgets all the objects loaded; all the unsaved changes discarded
func (orm *ORM) Clear() error {
	orm.lock.Lock()
	if orm.Host == nil {
		orm.lock.Unlock()
		return ErrClosed
	}
	orm.objects = make(map[rdx.ID]NativeObject)
	orm.Snap = orm.Host.Database().NewSnapshot()
	orm.lock.Unlock()
	return nil
}

func (orm *ORM) Close() error {
	orm.lock.Lock()
	if orm.Host == nil {
		orm.lock.Unlock()
		return ErrClosed
	}
	orm.objects = nil
	orm.Host = nil
	_ = orm.Snap.Close()
	orm.Snap = nil
	orm.lock.Unlock()
	return nil
}

func (orm *ORM) UpdateObject(obj NativeObject, snap *pebble.Snapshot) error {
	id := orm.FindID(obj)
	if id == rdx.BadId {
		return ErrObjectUnknown
	}
	it, err := ObjectIterator(id, snap)
	if err != nil {
		return err
	}
	seq := orm.Snap.Seq()
	for it.Next() {
		lid, rdt := OKeyIdRdt(it.Key())
		off := lid.Off()
		if it.Seq() > seq {
			e := obj.Load(off, rdt, it.Value())
			if e != nil { // the db may have garbage
				_ = 0 // todo
			}
		}
	}
	_ = it.Close()
	return err
}

func (orm *ORM) UpdateAll() (err error) {
	snap := orm.Host.Database().NewSnapshot()
	orm.lock.Lock()
	for _, obj := range orm.objects {
		err = orm.UpdateObject(obj, snap)
		if err != nil {
			break
		}
	}
	orm.lock.Unlock()
	return
}

// Saves all the changes, takes a new snapshot, updates
func (orm *ORM) SyncAll() (err error) {
	err = orm.SaveAll()
	if err == nil {
		err = orm.UpdateAll()
	}
	return
}

func (orm *ORM) Load(id rdx.ID, blanc NativeObject) (obj NativeObject, err error) {
	io := pebble.IterOptions{
		LowerBound: []byte{},
		UpperBound: []byte{},
	}
	it := orm.Snap.NewIter(&io)
	err = orm.UpdateObject(blanc, orm.Snap)
	orm.lock.Lock()
	if err == nil {
		orm.objects[id] = blanc
	}
	orm.lock.Unlock()
	_ = it.Close()
	return
}

func (orm *ORM) Object(id rdx.ID) (obj NativeObject) {
	orm.lock.Lock()
	obj = orm.objects[id]
	orm.lock.Unlock()
	return // todo
}

func (orm *ORM) FindID(obj NativeObject) rdx.ID {
	orm.lock.Lock()
	id, ok := orm.ids[obj]
	orm.lock.Unlock()
	if !ok {
		id = rdx.BadId
	}
	return id
}

type templateState struct {
	Name    string
	Fields  Fields
	Natives map[byte]string
}

func (orm *ORM) Compile(name string, cid rdx.ID) (code string, err error) {
	class, e := template.New("test").Parse(ClassTemplate)
	if e != nil {
		return "", e
	}
	var state templateState
	state.Fields, err = orm.Host.ClassFields(cid)
	if err != nil {
		return
	}
	state.Natives = FIRSTnatives
	state.Name = name
	buf := bytes.Buffer{}
	err = class.Execute(&buf, state)
	if err == nil {
		code = buf.String()
	}
	return
}

var FIRSTnatives = map[byte]string{
	'F': "float64",
	'I': "int64",
	'R': "rdx.ID",
	'S': "string",
	'T': "string",
}

var ClassTemplate = `
{{$n := .Natives}}
type {{ .Name }} struct {
	{{ range .Fields }}
	{{ .Name }} {{ index $n .RdxType }}
	{{ end }}
}

func (o *{{.Name}}) Load(off uint64, rdt byte, tlv []byte) error {
	switch (off) {
	{{ range $n, $f := .Fields }}
    case {{$n}}:
		if rdt != {{$f.RdxType}} { break }
		o.{{$f.Name}} = rdx.{{printf "%cnative" $f.RdxType }}(tlv)
	{{ end }}
	default: return ErrUnknownFieldInAType
	}
	return nil
}

func (o *{{.Name}}) Store(off uint64, rdt byte, old []byte) (changes []byte, err error) {
	switch (off) {
	{{ range $n, $f := .Fields }}
    case {{$n}}:
		if rdt != {{$f.RdxType}} { break }
		d := rdx.{{printf "%cdelta" $f.RdxType }}(i.Value(), o.{{$f.Name}})
		delta = append(delta, chotki.EditTLV({{$n}}, {{$f.RdxType}}, d))
	{{ end }}
	default: return ErrUnknownFieldInAType
	}
    return nil
}
`

// todo collection description
var ETemplate = `
func (o *{{Name}}) Get{{- Name}}() {
}

func (o *{{Name}}) Put{{- Name}}() {
}
`
