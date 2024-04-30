package chotki

import (
	"bytes"
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/toyqueue"
	"github.com/drpcorg/chotki/toytlv"
	"sync"
	"text/template"
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
	objects map[rdx.ID]NativeObject
	ids     map[NativeObject]rdx.ID
	lock    sync.Mutex
}

func (orm *ORM) New(cid rdx.ID, objs ...NativeObject) (err error) {
	fields, e := orm.Host.ClassFields(cid)
	if e != nil {
		return e
	}
	for _, obj := range objs {
		tlv := toyqueue.Records{}
		for i := 1; i < len(fields) && err == nil; i++ {
			field := &fields[i]
			var edit []byte
			edit, err = obj.Store(uint64(i), field.RdxType, nil, orm.Host.Clock())
			tlv = append(tlv, toytlv.Record(field.RdxType, edit))
		}
		var id rdx.ID
		id, err = orm.Host.CommitPacket('O', cid, tlv)
		if err == nil {
			orm.lock.Lock()
			orm.ids[obj] = id
			orm.objects[id] = obj
			orm.lock.Unlock()
		}
	}
	return nil
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
			change, e := obj.Store(off, rdt, it.Value(), orm.Host.Clock())
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
			change, _ := obj.Store(uint64(off), fields[off].RdxType, nil, orm.Host.Clock())
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
	orm.lock.Lock()
	pre, ok := orm.objects[id]
	orm.lock.Unlock()
	if ok {
		return pre, nil
	}
	fro, til := ObjectKeyRange(id)
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: til,
	}
	it := orm.Snap.NewIter(&io)
	for it.SeekGE(fro); it.Valid(); it.Next() {
		lid, rdt := OKeyIdRdt(it.Key())
		off := lid.Off()
		e := blanc.Load(off, rdt, it.Value())
		if e != nil { // the db may have garbage
			_ = 0 // todo
		}
	}
	_ = it.Close()
	orm.lock.Lock()
	pre, ok = orm.objects[id]
	if ok {
		orm.lock.Unlock()
		return pre, nil
	}
	orm.objects[id] = blanc
	orm.ids[blanc] = id
	orm.lock.Unlock()
	return blanc, nil
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
	CId     rdx.ID
	Name    string
	Fields  Fields
	Natives map[byte]string
}

func (orm *ORM) Compile(name string, cid rdx.ID) (code string, err error) {
	class, e := template.New("test").Parse(ClassTemplate)
	if e != nil {
		return "", e
	}
	state := templateState{
		CId:     cid,
		Natives: FIRSTnatives,
		Name:    name,
	}
	state.Fields, err = orm.Host.ClassFields(cid)
	if err != nil {
		return
	}
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
	'N': "uint64",
	'Z': "int64",
}

// todo RDX formula
var ClassTemplate = `
{{$nat := .Natives}}
type {{ .Name }} struct {
	{{ range $n, $f := .Fields }}
		{{ if eq $n 0 }} {{continue}} {{end }}
		{{ $f.Name }} {{ index $nat $f.RdxType }}
	{{ end }}
}

var {{.Name}}ClassId = rdx.IDFromString("{{.CId.String}}")

func (o *{{.Name}}) Load(off uint64, rdt byte, tlv []byte) error {
	switch (off) {
	{{ range $n, $f := .Fields }}
	{{ if eq $n 0 }} {{continue}} {{end }}
    case {{$n}}:
		{{ $rdt := printf "%c" $f.RdxType  }}
		if rdt != '{{$rdt}}' { break }
		o.{{$f.Name}} = rdx.{{$rdt}}native(tlv)
	{{ end }}
	default: return chotki.ErrUnknownFieldInAType
	}
	return nil
}

func (o *{{.Name}}) Store(off uint64, rdt byte, old []byte, clock rdx.Clock) (bare []byte, err error) {
	switch (off) {
	{{ range $n, $f := .Fields }}
	{{ if eq $n 0 }} {{continue}} {{end }}
    case {{$n}}:
		{{ $rdt := printf "%c" $f.RdxType  }}
		if rdt != '{{$rdt}}' { break }
		if old == nil {
            bare = rdx.{{$rdt}}tlv(o.{{$f.Name}})
		} else {
			bare = rdx.{{$rdt}}delta(old, o.{{$f.Name}}, clock)
		}
	{{ end }}
	default: return nil, chotki.ErrUnknownFieldInAType
	}
	return 
}
`

// todo collection description
var ETemplate = `
func (o *{{Name}}) Get{{- Name}}() {
}

func (o *{{Name}}) Put{{- Name}}() {
}
`
