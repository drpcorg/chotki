package chotki

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/pkg/errors"
)

func OKey(id rdx.ID, rdt byte) (key []byte) {
	var ret = [16]byte{'O'}
	key = binary.BigEndian.AppendUint64(ret[:1], uint64(id))
	key = append(key, rdt)
	return
}

const LidLKeyLen = 1 + 8 + 1

func OKeyIdRdt(key []byte) (id rdx.ID, rdt byte) {
	if len(key) != LidLKeyLen {
		return rdx.BadId, 0
	}

	id = rdx.IDFromBytes(key[1 : LidLKeyLen-1])
	rdt = key[LidLKeyLen-1]
	return
}

func VKey(id rdx.ID) (key []byte) {
	var ret = [16]byte{'V'}
	block := id & ^SyncBlockMask
	key = binary.BigEndian.AppendUint64(ret[:1], uint64(block))
	key = append(key, 'V')
	return
}

func VKeyId(key []byte) rdx.ID {
	if len(key) != LidLKeyLen {
		return rdx.BadId
	}
	return rdx.IDFromBytes(key[1:])
}

type Field struct {
	Name    string
	RdxType byte
}

func (f Field) Valid() bool {
	for _, l := range f.Name { // has unsafe chars
		if l < ' ' {
			return false
		}
	}

	return (f.RdxType >= 'A' && f.RdxType <= 'Z' &&
		len(f.Name) > 0 && utf8.ValidString(f.Name))
}

type Fields []Field

func (f Fields) Find(name string) (ndx int) {
	for i := 0; i < len(f); i++ {
		if f[i].Name == name {
			return i
		}
	}
	return -1
}

func ObjectKeyRange(oid rdx.ID) (fro, til []byte) {
	oid = oid & ^rdx.OffMask
	return OKey(oid, 'O'), OKey(oid+rdx.ProInc, 0)
}

// returns nil for "not found"
func (cho *Chotki) ObjectIterator(oid rdx.ID, snap *pebble.Snapshot) *pebble.Iterator {
	fro, til := ObjectKeyRange(oid)
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: til,
	}
	var it *pebble.Iterator
	if snap != nil {
		it = snap.NewIter(&io)
	} else {
		it = cho.db.NewIter(&io)
	}

	if it.SeekGE(fro) { // fixme
		id, rdt := OKeyIdRdt(it.Key())
		if rdt == 'O' && id == oid {
			// An iterator is returned from a function, it cannot be closed
			return it
		}
	}
	if it != nil {
		_ = it.Close()
	}
	return nil
}

// todo []Field -> map[uint64]Field
func (cho *Chotki) ClassFields(cid rdx.ID) (fields Fields, err error) {
	fields, ok := cho.types.Load(cid)
	if ok {
		return
	}
	okey := OKey(cid, 'C')
	tlv, clo, e := cho.db.Get(okey)
	if e != nil {
		return nil, ErrTypeUnknown
	}
	it := rdx.FIRSTIterator{TLV: tlv}
	if !it.Next() {
		return nil, ErrBadClass
	}
	rr, _, rv := it.ParsedValue()
	if rr != rdx.Term || string(rv) != "_ref" || !it.Next() {
		return nil, ErrBadClass
	}
	pr, _, pv := it.ParsedValue()
	if pr != rdx.Reference {
		return nil, ErrBadClass
	}
	fields = append(fields, Field{Name: "_ref", RdxType: rdx.Reference})
	_ = pv // todo recur
	for it.Next() {
		lit, _, name := it.ParsedValue()
		if lit != rdx.Term || len(name) == 0 || !it.Next() {
			break // todo unique names etc
		}
		lit2, _, rdt := it.ParsedValue()
		if lit2 != rdx.Term || len(rdt) == 0 {
			break
		}
		fields = append(fields, Field{
			RdxType: rdt[0],
			Name:    string(name),
		})
	}
	_ = clo.Close()
	cho.types.Store(cid, fields)
	return
}

func (cho *Chotki) ObjectFieldsByClass(oid rdx.ID, form []string) (tid rdx.ID, tlvs protocol.Records, err error) {
	it := cho.ObjectIterator(oid, nil)
	if it == nil {
		return rdx.BadId, nil, ErrObjectUnknown
	}
	defer it.Close()

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
		for off > len(tlvs)+1 {
			tlvs = append(tlvs, nil)
		}
		tlvs = append(tlvs, it.Value())
	}
	return
}

func (cho *Chotki) ObjectFields(oid rdx.ID) (tid rdx.ID, decl Fields, fact protocol.Records, err error) {
	it := cho.ObjectIterator(oid, nil)
	if it == nil {
		err = ErrObjectUnknown
		return
	}
	defer it.Close()

	tid = rdx.IDFromZipBytes(it.Value())
	decl, err = cho.ClassFields(tid)
	if err != nil {
		return
	}
	fact = append(fact, it.Value())
	for it.Next() {
		id, rdt := OKeyIdRdt(it.Key())
		off := int(id.Off())
		if off == 0 || off >= len(decl) {
			continue
		}
		if decl[off].RdxType != rdt {
			continue
		}
		for off > len(fact) {
			fact = append(fact, nil)
		}
		fact = append(fact, it.Value())
	}
	return
}

func (cho *Chotki) ObjectFieldsTLV(oid rdx.ID) (tid rdx.ID, tlv protocol.Records, err error) {
	it := cho.ObjectIterator(oid, nil)
	if it == nil {
		return rdx.BadId, nil, ErrObjectUnknown
	}
	defer it.Close()

	tid = rdx.IDFromZipBytes(it.Value())
	for it.Next() {
		cp := make([]byte, len(it.Value()))
		copy(cp, it.Value())
		tlv = append(tlv, cp)
	}
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

func (cho *Chotki) ObjectFieldTLV(fid rdx.ID) (rdt byte, tlv []byte, err error) {
	db := cho.db
	if db == nil {
		return 0, nil, ErrClosed
	}

	it := cho.db.NewIter(&pebble.IterOptions{})
	defer it.Close()

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
	return
}

func (cho *Chotki) NewClass(parent rdx.ID, fields ...Field) (id rdx.ID, err error) {
	var fspecs protocol.Records
	//fspecs = append(fspecs, protocol.Record('A', parent.ZipBytes()))
	for _, field := range fields {
		if !field.Valid() {
			return rdx.BadId, ErrBadTypeDescription
		}
		fspecs = append(fspecs, protocol.Record('T', rdx.Ttlv(field.Name)))
		fspecs = append(fspecs, protocol.Record('T', rdx.Ttlv(string(field.RdxType))))
	}
	return cho.CommitPacket('C', parent, fspecs)
}

func (cho *Chotki) NewObject(tid rdx.ID, fields ...string) (id rdx.ID, err error) {
	var form Fields
	form, err = cho.ClassFields(tid)
	if err != nil {
		return
	}
	if len(fields) > len(form) {
		return rdx.BadId, ErrUnknownFieldInAType
	}
	var packet protocol.Records
	for i := 0; i < len(fields); i++ {
		rdt := form[i+1].RdxType
		tlv := rdx.Xparse(rdt, fields[i])
		if tlv == nil {
			return rdx.BadId, ErrBadValueForAType
		}
		packet = append(packet, protocol.Record(rdt, tlv))
	}
	return cho.CommitPacket('O', tid, packet)
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
	var packet protocol.Records
	for i := 0; i < len(fields); i++ {
		rdt := byte(formula[i].RdxType)
		tlv := rdx.X2string(rdt, obj[i], fields[i], cho.src)
		if tlv == nil {
			return rdx.BadId, ErrBadValueForAType
		}
		packet = append(packet, protocol.Record('F', rdx.ZipUint64(uint64(i))))
		packet = append(packet, protocol.Record(rdt, tlv))
	}
	return cho.CommitPacket('E', oid, packet)
}

/*func (cho *Chotki) GetObject(oid rdx.ID) (tid rdx.ID, fields []string, err error) {
	i := cho.ObjectIterator(oid)
	if i == nil || !i.Valid() {
		return rdx.BadId, nil, ErrObjectUnknown
	}
	tid = rdx.IDFromZipBytes(i.Value())
	for i.Next() {
		_, rdt := OKeyIdRdt(i.Key())
		str := rdx.Xstring(rdt, i.Value())
		fields = append(fields, str)
	}
	return
}*/

func (cho *Chotki) ObjectString(oid rdx.ID) (txt string, err error) {
	_, form, fact, e := cho.ObjectFields(oid)
	if e != nil {
		return "", e
	}
	ret := []byte{'{'}
	for n, d := range form {
		if n != 0 {
			ret = append(ret, ',')
		}
		ret = append(ret, d.Name...)
		ret = append(ret, ':')
		ret = append(ret, rdx.Xstring(d.RdxType, fact[n])...)
	}
	ret = append(ret, '}')
	txt = string(ret)
	return
}

func (cho *Chotki) EditObjectRDX(oid rdx.ID, pairs []rdx.RDX) (id rdx.ID, err error) {
	tlvs := protocol.Records{}
	_, form, fact, e := cho.ObjectFields(oid)
	if e != nil {
		return rdx.BadId, e
	}
	tmp := make(protocol.Records, len(fact))
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i].RdxType != rdx.Term {
			return
		}
		name := pairs[i].String()
		value := &pairs[i+1]
		ndx := form.Find(name)
		if ndx == -1 {
			err = fmt.Errorf("unknown field %s", name)
			return
		}
		tmp[ndx] = rdx.FIRSTrdx2tlv(value)
	}
	for i := 0; i < len(form); i++ {
		if tmp[i] != nil {
			tlvs = append(tlvs, protocol.TinyRecord('F', rdx.ZipUint64(uint64(i))))
			tlvs = append(tlvs, tmp[i])
		}
	}
	return cho.CommitPacket('E', oid, tlvs)
}

func (cho *Chotki) SetFieldTLV(fid rdx.ID, tlve []byte) (id rdx.ID, err error) {
	oid := fid.ZeroOff()
	f := protocol.Record('F', rdx.ZipUint64(uint64(fid.Off())))
	return cho.CommitPacket('E', oid, protocol.Records{f, tlve})
}

var ErrWrongFieldType = errors.New("wrong field type")

func (cho *Chotki) AddToNField(fid rdx.ID, count uint64) (id rdx.ID, err error) {
	rdt, tlv, err := cho.ObjectFieldTLV(fid)
	if err != nil || rdt != rdx.Natural {
		return rdx.BadId, ErrWrongFieldType
	}
	src := cho.Source()
	mine := rdx.Nmine(tlv, src)
	tlvs := protocol.Records{
		protocol.Record('F', rdx.ZipUint64(fid.Off())),
		protocol.Record(rdx.Natural, rdx.Ntlvt(mine+count, src)),
	}
	id, err = cho.CommitPacket('E', fid.ZeroOff(), tlvs)
	return
}

func (cho *Chotki) IncNField(fid rdx.ID) (id rdx.ID, err error) {
	return cho.AddToNField(fid, 1)
}

func (cho *Chotki) ObjectFieldMapTermId(fid rdx.ID) (themap rdx.MapTR, err error) {
	rdt, tlv, e := cho.ObjectFieldTLV(fid)
	if e != nil {
		return nil, e
	}
	if rdt != rdx.Mapping {
		return nil, ErrWrongFieldType
	}
	themap = rdx.MnativeTR(tlv)
	return
}

func (cho *Chotki) GetFieldTLV(id rdx.ID) (rdt byte, tlv []byte) {
	key := OKey(id, 'A')
	it := cho.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	})
	defer it.Close()
	if it.SeekGE(key) {
		fact, r := OKeyIdRdt(it.Key())
		if fact == id {
			tlv = it.Value()
			rdt = r
		}
	}
	return
}

func EditTLV(off uint64, rdt byte, tlv []byte) (edit []byte) {
	edit = append(edit, protocol.TinyRecord('F', rdx.ZipUint64(off))...)
	edit = append(edit, protocol.Record(rdt, tlv)...)
	return
}

func (cho *Chotki) EditFieldTLV(fid rdx.ID, delta []byte) (id rdx.ID, err error) {
	tlvs := protocol.Records{}
	tlvs = append(tlvs, protocol.TinyRecord('F', rdx.ZipUint64(fid.Off())))
	tlvs = append(tlvs, delta)
	id, err = cho.CommitPacket('E', fid.ZeroOff(), tlvs)
	return
}
