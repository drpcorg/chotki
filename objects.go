package chotki

import (
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/pkg/errors"
	"unicode/utf8"
)

var ErrBadTypeDescription = errors.New("bad type description")

func hasUnsafeChars(text string) bool {
	for _, l := range text {
		if l < ' ' {
			return true
		}
	}
	return false
}

var ErrUnknownObject = errors.New("unknown object")
var ErrTypeUnknown = errors.New("unknown object type")
var ErrUnknownFieldInAType = errors.New("unknown field for the type")
var ErrBadValueForAType = errors.New("bad value for the type")

func (cho *Chotki) ClassFields(tid rdx.ID) (fields []string, err error) {
	fields, ok := cho.types[tid]
	if ok {
		return
	}
	i := cho.ObjectIterator(tid)
	if i == nil {
		return nil, ErrTypeUnknown
	}
	for i.Next() { // skip the parent
		fields = append(fields, rdx.Snative(i.Value()))
	}
	cho.lock.Lock()
	cho.types[tid] = fields
	cho.lock.Unlock()
	return
}

func (cho *Chotki) ObjectFieldsByClass(oid rdx.ID, form []string) (tid rdx.ID, tlv toyqueue.Records, err error) {
	it := cho.ObjectIterator(oid)
	if it == nil {
		return rdx.BadId, nil, ErrUnknownObject
	}
	tid = rdx.IDFromZipBytes(it.Value())
	for it.Next() {
		id, rdt := OKeyIdRdt(it.Key())
		off := int(id.Off())
		if off == 0 || off > len(form) {
			continue
		}
		decl := form[off-1]
		if len(decl) == 0 || rdt != decl[0] {
			continue
		}
		for off > len(tlv)+1 {
			tlv = append(tlv, nil)
		}
		tlv = append(tlv, it.Value())
	}
	_ = it.Close()
	return
}

func (cho *Chotki) ObjectFieldsTLV(oid rdx.ID) (tid rdx.ID, tlv toyqueue.Records, err error) {
	it := cho.ObjectIterator(oid)
	if it == nil {
		return rdx.BadId, nil, ErrUnknownObject
	}
	tid = rdx.IDFromZipBytes(it.Value())
	for it.Next() {
		cp := make([]byte, len(it.Value()))
		copy(cp, it.Value())
		tlv = append(tlv, cp)
	}
	_ = it.Close()
	return
}

func FieldOffset(fields []string, name string) rdx.ID {
	for i := 0; i < len(fields); i++ {
		fn := fields[i]
		if len(fn) > 0 && fn[1:] == name {
			return rdx.ID(i + 1)
		}
	}
	return 0
}

// returns 0 if no such field found
func (cho *Chotki) FindFieldOffset(oid rdx.ID, name string) (off rdx.ID) {
	fields, err := cho.ClassFields(oid)
	if err != nil {
		return 0
	}
	return FieldOffset(fields, name)
}

func (cho *Chotki) ObjectField(oid rdx.ID, off rdx.ID) (rdt byte, tlv []byte, err error) {
	it := cho.db.NewIter(&pebble.IterOptions{})
	fid := (oid &^ rdx.OffMask) + off
	key := OKey(fid, 0)
	if !it.SeekGE(key) {
		return 0, nil, pebble.ErrNotFound
	}
	var fidfact rdx.ID
	fidfact, rdt = OKeyIdRdt(it.Key())
	if fidfact != fid {
		return 0, nil, pebble.ErrNotFound
	}
	tlv = it.Value()
	_ = it.Close()
	return
}

func (cho *Chotki) NewClass(parent rdx.ID, fields ...string) (id rdx.ID, err error) {
	var fspecs toyqueue.Records
	//fspecs = append(fspecs, toytlv.Record('A', parent.ZipBytes()))
	for _, field := range fields {
		if len(field) < 2 || field[0] < 'A' || field[0] > 'Z' || !utf8.ValidString(field) || hasUnsafeChars(field) {
			return rdx.BadId, ErrBadTypeDescription
		}
		fspecs = append(fspecs, toytlv.Record('S', rdx.Stlv(field)))
	}
	return cho.CommitPacket('C', parent, fspecs)
}

func (cho *Chotki) NewObject(oid rdx.ID, fields ...string) (id rdx.ID, err error) {
	var formula []string
	formula, err = cho.ClassFields(oid)
	if err != nil {
		return
	}
	if len(fields) > len(formula) {
		return rdx.BadId, ErrUnknownFieldInAType
	}
	var packet toyqueue.Records
	for i := 0; i < len(fields); i++ {
		rdt := formula[i][0]
		tlv := rdx.Xparse(rdt, fields[i])
		if tlv == nil {
			return rdx.BadId, ErrBadValueForAType
		}
		packet = append(packet, toytlv.Record(rdt, tlv))
	}
	return cho.CommitPacket('O', oid, packet)
}

func (cho *Chotki) EditObject(oid rdx.ID, fields ...string) (id rdx.ID, err error) {
	formula, err := cho.ClassFields(oid)
	if err != nil {
		return rdx.BadId, err
	}
	if len(fields) > len(formula) {
		return rdx.BadId, ErrUnknownFieldInAType
	}
	_, obj, err := cho.ObjectFieldsTLV(oid)
	if err != nil {
		return rdx.BadId, err
	}
	// fetch type desc
	var packet toyqueue.Records
	for i := 0; i < len(fields); i++ {
		rdt := formula[i][0]
		tlv := rdx.X2string(rdt, obj[i], fields[i], cho.src)
		if tlv == nil {
			return rdx.BadId, ErrBadValueForAType
		}
		packet = append(packet, toytlv.Record('F', rdx.ZipUint64(uint64(i))))
		packet = append(packet, toytlv.Record(rdt, tlv))
	}
	return cho.CommitPacket('E', oid, packet)
}

func (cho *Chotki) GetObject(oid rdx.ID) (tid rdx.ID, fields []string, err error) {
	i := cho.ObjectIterator(oid)
	if i == nil || !i.Valid() {
		return rdx.BadId, nil, ErrUnknownObject
	}
	tid = rdx.IDFromZipBytes(i.Value())
	for i.Next() {
		_, rdt := OKeyIdRdt(i.Key())
		str := rdx.Xstring(rdt, i.Value())
		fields = append(fields, str)
	}
	return
}
