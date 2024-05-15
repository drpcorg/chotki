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

// A class contains a number of fields. Each Field has
// some RDT type. A class can inherit another class.
// New fields can be appended to a class, but never removed.
// Max number of fields is 128, max inheritance depth 32.
// When stored, a class is an append-only sequence of Ts.
// The syntax for each T: "XName", where X is the RDT.
// For the map types, can use "MSS_Name" or similar.
// Each field has an Offset. The Offset+RdxType pair is the
// *actual key* for the field in the database.
// Entries having identical Offset+RdxType are considered *renames*!
type Field struct {
	Offset     int64
	Name       string
	RdxType    byte
	RdxTypeExt []byte
}

// Fields
type Fields []Field

func (f Field) Valid() bool {
	for _, l := range f.Name { // has unsafe chars
		if l < ' ' {
			return false
		}
	}

	return (f.RdxType >= 'A' && f.RdxType <= 'Z' &&
		len(f.Name) > 0 && utf8.ValidString(f.Name))
}

func (fs Fields) MaxOffset() (off int64) {
	for _, f := range fs {
		if f.Offset > off {
			off = f.Offset
		}
	}
	return
}

func (f Fields) FindRdtOff(rdx byte, off int64) int {
	for i := 0; i < len(f); i++ {
		if f[i].RdxType == rdx && f[i].Offset == off {
			return i
		}
	}
	return -1
}

func (f Fields) FindName(name string) (ndx int) { // fixme double naming?
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

// todo note that the class may change as the program runs; in such a case
// if the class fields are already cached, the current session will not
// understand the new fields!
func (cho *Chotki) ClassFields(cid rdx.ID) (fields Fields, err error) {
	if fields, ok := cho.types.Load(cid); ok {
		return fields, nil
	}

	okey := OKey(cid, 'C')
	tlv, clo, e := cho.db.Get(okey)
	if e != nil {
		return nil, ErrTypeUnknown
	}
	it := rdx.FIRSTIterator{TLV: tlv}
	fields = append(fields, Field{ // todo inheritance
		Offset:  0,
		Name:    "_ref",
		RdxType: rdx.Reference,
	})
	for it.Next() {
		lit, t, name := it.ParsedValue()
		if lit != rdx.Term || len(name) == 0 {
			break // todo unique names etc
		}
		rdt := rdx.String
		if name[0] >= 'A' && name[0] <= 'Z' {
			rdt = name[0]
			name = name[1:]
		}
		fields = append(fields, Field{
			Offset:  t.Rev,
			RdxType: rdt,
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
	fact = make(protocol.Records, len(decl))
	if err != nil {
		return
	}
	fact = append(fact, it.Value())
	for it.Next() {
		id, rdt := OKeyIdRdt(it.Key())
		off := int64(id.Off())
		ndx := decl.FindRdtOff(rdt, off)
		if ndx == -1 {
			continue
		}
		fact[ndx] = it.Value()
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

// ObjectFieldTLV picks one field fast. No class checks, etc.
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
	maxidx := int64(-1)
	for _, field := range fields {
		if field.Offset > maxidx {
			maxidx = field.Offset
		} else if field.Offset == 0 {
			maxidx++
			field.Offset = maxidx
		}
		if !field.Valid() {
			return rdx.BadId, ErrBadTypeDescription
		}
		name := append([]byte{}, field.RdxType)
		name = append(name, field.Name...)
		fspecs = append(fspecs, protocol.Record('T', rdx.FIRSTtlv(maxidx, 0, name)))
	}
	//head := protocol.AppendHeader(nil) fspecs.TotalLen()
	return cho.CommitPacket('C', parent, fspecs)
}

// Creates a new object from enveloped TLV fields; no class checks.
func (cho *Chotki) NewObjectTLV(tid rdx.ID, fields protocol.Records) (id rdx.ID, err error) {
	return cho.CommitPacket('O', tid, fields)
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
			return rdx.BadId, rdx.ErrBadValueForAType
		}
		packet = append(packet, protocol.Record(rdt, tlv))
	}
	return cho.NewObjectTLV(tid, packet)
}

// Deprecated: does not handle non-trivial cases
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
			return rdx.BadId, rdx.ErrBadValueForAType
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
		ndx := form.FindName(name)
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

func (cho *Chotki) MapTRField(fid rdx.ID) (themap rdx.MapTR, err error) {
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

func (cho *Chotki) MapSSField(fid rdx.ID) (themap rdx.MapSS, err error) {
	rdt, tlv, e := cho.ObjectFieldTLV(fid)
	if e != nil {
		return nil, e
	}
	if rdt != rdx.Mapping {
		return nil, ErrWrongFieldType
	}
	themap = rdx.MnativeSS(tlv)
	return
}

// Adds/removes elements to/from a map (removed should map to nil)
func (cho *Chotki) AddToMapTRField(fid rdx.ID, changes rdx.MapTR) (id rdx.ID, err error) {
	rdt, tlv := cho.GetFieldTLV(fid) // todo error?
	if rdt != rdx.Mapping {
		return rdx.BadId, ErrWrongFieldType
	}
	newtlv := rdx.MtlvTR(changes)
	dtlv := rdx.Mdelta2(tlv, newtlv)
	if len(dtlv) == 0 {
		return rdx.ID0, nil
	}
	packet := protocol.Records{
		protocol.Record('F', rdx.ZipUint64(fid.Off())),
		protocol.Record(rdx.Mapping, dtlv),
	}
	id, err = cho.CommitPacket('E', fid.ZeroOff(), packet)
	return
}

func (cho *Chotki) SetMapTRField(fid rdx.ID, changes rdx.MapTR) (id rdx.ID, err error) {
	rdt, tlv := cho.GetFieldTLV(fid) // todo error?
	if rdt != rdx.Mapping {
		return rdx.BadId, ErrWrongFieldType
	}
	newtlv := rdx.MtlvTR(changes)
	dtlv := rdx.Mdelta(tlv, newtlv)
	if len(dtlv) == 0 {
		return rdx.ID0, nil
	}
	packet := protocol.Records{
		protocol.Record('F', rdx.ZipUint64(fid.Off())),
		protocol.Record(rdx.Mapping, dtlv),
	}
	id, err = cho.CommitPacket('E', fid.ZeroOff(), packet)
	return
}

func (cho *Chotki) AddToMapSSField(fid rdx.ID, changes rdx.MapSS) (id rdx.ID, err error) {
	rdt, tlv := cho.GetFieldTLV(fid) // todo error?
	if rdt != rdx.Mapping {
		return rdx.BadId, ErrWrongFieldType
	}
	newtlv := rdx.MtlvSS(changes)
	dtlv := rdx.Mdelta2(tlv, newtlv)
	if len(dtlv) == 0 {
		return rdx.ID0, nil
	}
	packet := protocol.Records{
		protocol.Record('F', rdx.ZipUint64(fid.Off())),
		protocol.Record(rdx.Mapping, dtlv),
	}
	id, err = cho.CommitPacket('E', fid.ZeroOff(), packet)
	return
}

func (cho *Chotki) SetMapSSField(fid rdx.ID, changes rdx.MapSS) (id rdx.ID, err error) {
	rdt, tlv := cho.GetFieldTLV(fid) // todo error?
	if rdt != rdx.Mapping {
		return rdx.BadId, ErrWrongFieldType
	}
	newtlv := rdx.MtlvSS(changes)
	dtlv := rdx.Mdelta(tlv, newtlv)
	if len(dtlv) == 0 {
		return rdx.ID0, nil
	}
	packet := protocol.Records{
		protocol.Record('F', rdx.ZipUint64(fid.Off())),
		protocol.Record(rdx.Mapping, dtlv),
	}
	id, err = cho.CommitPacket('E', fid.ZeroOff(), packet)
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
