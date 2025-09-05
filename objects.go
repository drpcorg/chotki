package chotki

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/chotki_errors"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/pkg/errors"
)

// Creates and iterator that only walks through the specific object.
// Will return nil if the object does not exist.
func (cho *Chotki) ObjectIterator(oid rdx.ID, snap *pebble.Snapshot) *pebble.Iterator {
	fro, til := host.ObjectKeyRange(oid)
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: til,
	}
	var it *pebble.Iterator
	var err error
	if snap != nil {
		it, err = snap.NewIter(&io)
		if err != nil {
			return nil
		}
	} else {
		it, err = cho.db.NewIter(&io)
		if err != nil {
			return nil
		}
	}

	if it.SeekGE(fro) { // fixme
		id, rdt := host.OKeyIdRdt(it.Key())
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

// Returns class fields and caches them. If class is changed cache will be invalidated.
func (cho *Chotki) ClassFields(cid rdx.ID) (fields classes.Fields, err error) {
	if fields, ok := cho.types.Load(cid); ok {
		return fields, nil
	}

	okey := host.OKey(cid, 'C')
	tlv, clo, e := cho.db.Get(okey)
	if e != nil {
		return nil, chotki_errors.ErrTypeUnknown
	}
	fields = classes.ParseClass(tlv)
	_ = clo.Close()
	cho.types.Store(cid, fields)
	return
}

// Given object id, returns class definition and actual object fields values
func (cho *Chotki) ObjectFields(oid rdx.ID) (tid rdx.ID, decl classes.Fields, fact protocol.Records, err error) {
	it := cho.ObjectIterator(oid, nil)
	if it == nil {
		err = chotki_errors.ErrObjectUnknown
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
		id, rdt := host.OKeyIdRdt(it.Key())
		off := int64(id.Off())
		ndx := decl.FindRdtOff(rdt, off)
		if ndx == -1 {
			continue
		}
		fact[ndx] = it.Value()
	}
	return
}

// Returns the TLV-encoded value of the object field, given object rdx.ID with offset.
func (cho *Chotki) ObjectFieldTLV(fid rdx.ID) (rdt byte, tlv []byte, err error) {
	db := cho.db
	if db == nil {
		return 0, nil, chotki_errors.ErrClosed
	}

	it, err := cho.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, nil, err
	}
	defer it.Close()

	key := host.OKey(fid, 0)
	if !it.SeekGE(key) {
		return 0, nil, pebble.ErrNotFound
	}
	var fidfact rdx.ID
	fidfact, rdt = host.OKeyIdRdt(it.Key())
	if fidfact != fid {
		return 0, nil, pebble.ErrNotFound
	}
	tlv = it.Value()
	return
}

// Creates or updates class definition. Remember that you can only add fields, not remove them.
func (cho *Chotki) NewClass(ctx context.Context, parent rdx.ID, fields ...classes.Field) (id rdx.ID, err error) {
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
		if field.Index == classes.FullscanIndex {
			return rdx.BadId, chotki_errors.ErrFullscanIndexField
		}
		if field.Index == classes.HashIndex && !rdx.IsFirst(field.RdxType) {
			return rdx.BadId, chotki_errors.ErrHashIndexFieldNotFirst
		}
		name := append([]byte{}, field.RdxType)
		name = append(name, byte(field.Index))
		name = append(name, field.Name...)
		fspecs = append(fspecs, protocol.Record('T', rdx.FIRSTtlv(maxidx, 0, name)))
	}
	return cho.CommitPacket(ctx, 'C', parent, fspecs)
}

// Returns the TLV-encoded class definition. Only used in REPL.
func (cho *Chotki) GetClassTLV(ctx context.Context, cid rdx.ID) ([]byte, error) {
	okey := host.OKey(cid, 'C')
	tlv, clo, e := cho.db.Get(okey)
	if e != nil {
		return nil, chotki_errors.ErrTypeUnknown
	}
	err := clo.Close()
	if err != nil {
		return nil, err
	}
	return tlv, nil
}

// Thin wrapper around CommitPacket with 'O' type
func (cho *Chotki) NewObjectTLV(ctx context.Context, tid rdx.ID, fields protocol.Records) (id rdx.ID, err error) {
	return cho.CommitPacket(ctx, 'O', tid, fields)
}

// Returns the string representation of the object.
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

// Edits the object fields using string RDX representation.
func (cho *Chotki) EditObjectRDX(ctx context.Context, oid rdx.ID, pairs []rdx.RDX) (id rdx.ID, err error) {
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
		tmp[ndx] = protocol.Record(value.RdxType, rdx.FIRSTrdx2tlv(value))
	}
	for i := 0; i < len(form); i++ {
		if tmp[i] != nil {
			tlvs = append(tlvs, protocol.TinyRecord('F', rdx.ZipUint64(uint64(i))))
			tlvs = append(tlvs, tmp[i])
		}
	}
	return cho.CommitPacket(ctx, 'E', oid, tlvs)
}

var ErrWrongFieldType = errors.New("wrong field type")

// Increments only 'N' counters by arbitrary amount.
func (cho *Chotki) AddToNField(ctx context.Context, fid rdx.ID, count uint64) (id rdx.ID, err error) {
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
	id, err = cho.CommitPacket(ctx, 'E', fid.ZeroOff(), tlvs)
	return
}

// Increments only 'N' counters by 1.
func (cho *Chotki) IncNField(ctx context.Context, fid rdx.ID) (id rdx.ID, err error) {
	return cho.AddToNField(ctx, fid, 1)
}

// Extracts TLV value of the field that is supposed to be a map and converts it to golang map.
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

// Returns the TLV-encoded value of the object field, given object rdx.ID with offset.
func (cho *Chotki) GetFieldTLV(id rdx.ID) (rdt byte, tlv []byte) {
	key := host.OKey(id, 'A')
	it, err := cho.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	})
	if err != nil {
		return 0, nil
	}
	defer it.Close()
	if it.SeekGE(key) {
		fact, r := host.OKeyIdRdt(it.Key())
		if fact == id {
			tlv = it.Value()
			rdt = r
		}
	}
	return
}

// Edits the object field using TLV-encoded value.
func (cho *Chotki) EditFieldTLV(ctx context.Context, fid rdx.ID, delta []byte) (id rdx.ID, err error) {
	tlvs := protocol.Records{}
	tlvs = append(tlvs, protocol.TinyRecord('F', rdx.ZipUint64(fid.Off())))
	tlvs = append(tlvs, delta)
	id, err = cho.CommitPacket(ctx, 'E', fid.ZeroOff(), tlvs)
	return
}
