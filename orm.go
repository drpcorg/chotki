package chotki

import (
	"bytes"
	"sync"
	"text/template"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/puzpuzpuz/xsync/v3"
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
	objects *xsync.MapOf[rdx.ID, NativeObject]
	ids     *xsync.MapOf[NativeObject, rdx.ID]
	lock    sync.Mutex
}

func NewORM(host *Chotki, snap *pebble.Snapshot) *ORM {
	return &ORM{
		Host: host,
		Snap: snap,

		ids:     xsync.NewMapOf[NativeObject, rdx.ID](),
		objects: xsync.NewMapOf[rdx.ID, NativeObject](),
	}
}

func (orm *ORM) New(cid rdx.ID, objs ...NativeObject) (err error) {
	fields, e := orm.Host.ClassFields(cid)
	if e != nil {
		return e
	}
	for _, obj := range objs {
		tlv := protocol.Records{}
		for i := 1; i < len(fields) && err == nil; i++ {
			field := &fields[i]
			var edit []byte
			edit, err = obj.Store(uint64(i), field.RdxType, nil)
			tlv = append(tlv, protocol.Record(field.RdxType, edit))
		}
		var id rdx.ID
		id, err = orm.Host.CommitPacket('O', cid, tlv)
		if err == nil {
			orm.ids.Store(obj, id)
			orm.objects.Store(id, obj)
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
		it := orm.Host.ObjectIterator(id, orm.Snap)
		if it == nil {
			err = ErrObjectUnknown
			break
		}
		cid := rdx.IDFromZipBytes(it.Value())
		fields, e := orm.Host.ClassFields(cid)
		if e != nil {
			return e
		}
		var changes protocol.Records
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
				changes = append(changes, protocol.Record('F', rdx.ZipUint64(off)))
				changes = append(changes, protocol.Record(rdt, change))
			}
		}
		for off := 1; off < len(fields); off++ {
			if flags[off] {
				continue
			}
			change, _ := obj.Store(uint64(off), fields[off].RdxType, nil)
			if change != nil {
				changes = append(changes, protocol.Record('F', rdx.ZipUint64(uint64(off))))
				changes = append(changes, protocol.Record(fields[off].RdxType, change))
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
	orm.objects.Range(func(_ rdx.ID, obj NativeObject) bool {
		err = orm.Save(obj)
		return err == nil
	})

	return
}

// Clear forgets all the objects loaded; all the unsaved changes discarded
func (orm *ORM) Clear() error {
	orm.lock.Lock()
	defer orm.lock.Unlock()

	if orm.Host == nil {
		return ErrClosed
	}
	orm.objects.Clear()
	orm.Snap = orm.Host.Database().NewSnapshot()
	return nil
}

func (orm *ORM) Close() error {
	orm.lock.Lock()
	defer orm.lock.Unlock()

	if orm.Host == nil {
		return ErrClosed
	}
	orm.objects.Clear()
	orm.Host = nil
	_ = orm.Snap.Close()
	orm.Snap = nil
	return nil
}

func (orm *ORM) UpdateObject(obj NativeObject, snap *pebble.Snapshot) error {
	id := orm.FindID(obj)
	if id == rdx.BadId {
		return ErrObjectUnknown
	}
	it := orm.Host.ObjectIterator(id, snap)
	if it == nil {
		return ErrObjectUnknown
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
	return nil
}

func (orm *ORM) UpdateAll() (err error) {
	snap := orm.Host.Database().NewSnapshot()
	orm.objects.Range(func(_ rdx.ID, obj NativeObject) bool {
		err = orm.UpdateObject(obj, snap)
		return err == nil
	})
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
	pre, ok := orm.objects.Load(id)
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
	pre, ok = orm.objects.Load(id)
	if ok {
		return pre, nil
	}
	orm.objects.Store(id, blanc)
	orm.ids.Store(blanc, id)
	return blanc, nil
}

func (orm *ORM) Object(id rdx.ID) (obj NativeObject) {
	obj, _ = orm.objects.Load(id)
	return // todo
}

func (orm *ORM) FindID(obj NativeObject) rdx.ID {
	id, ok := orm.ids.Load(obj)
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

func (o *{{.Name}}) Store(off uint64, rdt byte, old []byte) (bare []byte, err error) {
	switch (off) {
	{{ range $n, $f := .Fields }}
	{{ if eq $n 0 }} {{continue}} {{end }}
    case {{$n}}:
		{{ $rdt := printf "%c" $f.RdxType  }}
		if rdt != '{{$rdt}}' { break }
		if old == nil {
            bare = rdx.{{$rdt}}tlv(o.{{$f.Name}})
		} else {
			bare = rdx.{{$rdt}}delta(old, o.{{$f.Name}})
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
