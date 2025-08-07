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

// returns nil for "not found"
func (cho *Chotki) ObjectIterator(oid rdx.ID, snap *pebble.Snapshot) *pebble.Iterator {
	fro, til := host.ObjectKeyRange(oid)
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

// todo note that the class may change as the program runs; in such a case
// if the class fields are already cached, the current session will not
// understand the new fields!
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

// ObjectFieldTLV picks one field fast. No class checks, etc.
func (cho *Chotki) ObjectFieldTLV(fid rdx.ID) (rdt byte, tlv []byte, err error) {
	db := cho.db
	if db == nil {
		return 0, nil, chotki_errors.ErrClosed
	}

	it := cho.db.NewIter(&pebble.IterOptions{})
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

func (cho *Chotki) NewObjectTLV(ctx context.Context, tid rdx.ID, fields protocol.Records) (id rdx.ID, err error) {
	return cho.CommitPacket(ctx, 'O', tid, fields)
}

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

func (cho *Chotki) IncNField(ctx context.Context, fid rdx.ID) (id rdx.ID, err error) {
	return cho.AddToNField(ctx, fid, 1)
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

func (cho *Chotki) GetFieldTLV(id rdx.ID) (rdt byte, tlv []byte) {
	key := host.OKey(id, 'A')
	it := cho.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	})
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

func (cho *Chotki) EditFieldTLV(ctx context.Context, fid rdx.ID, delta []byte) (id rdx.ID, err error) {
	tlvs := protocol.Records{}
	tlvs = append(tlvs, protocol.TinyRecord('F', rdx.ZipUint64(fid.Off())))
	tlvs = append(tlvs, delta)
	id, err = cho.CommitPacket(ctx, 'E', fid.ZeroOff(), tlvs)
	return
}
