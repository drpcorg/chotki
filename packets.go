package main

import (
	"errors"
	"github.com/cockroachdb/pebble"
	"github.com/learn-decentralized-systems/toytlv"
)

func FieldID(oid ID, fno byte, rdt byte) ID {
	oid &= ID(^OffMask)
	oid |= ID(rdt - 'A')
	oid |= ID(fno) << RdtBits
	return oid
}

func (ch *Chotki) UpdateVTree(id, ref ID, pb *pebble.Batch) (err error) {
	err = pb.Merge(
		VKey(ref),
		toytlv.Record('V', id.ZipBytes()),
		&WriteOptions)
	if err != nil {
		return
	}
	err = pb.Merge(
		VKey(ID0),
		toytlv.Record('V', id.ZipBytes()),
		&WriteOptions)
	return
}

func (ch *Chotki) ApplyY(id, ref ID, body []byte, batch *pebble.Batch) (err error) {
	// see Chotki.SyncPeer()
	vvtlv, rest := toytlv.Take('V', body)
	if vvtlv == nil {
		return ErrBadYPacket
	}
	for len(rest) > 0 && err == nil {
		dzip, rest := toytlv.Take('F', rest)
		d := UnzipUint64(dzip)
		at := ref + ID(d) // fixme
		_, tlv, rest := toytlv.TakeAny(rest)
		err = batch.Merge(OKey(at), tlv, &WriteOptions)
	}
	return
}

func (ch *Chotki) ApplyV(id, ref ID, body []byte, batch *pebble.Batch) (err error) {
	_key := [32]byte{'V'}
	rest := body
	for len(rest) > 0 {
		var rec, idb []byte
		rec, rest = toytlv.Take('V', rest)
		idb, rec = toytlv.Take('R', rec)
		id := IDFromZipBytes(idb)
		key := append(_key[:1], id.Bytes()...)
		if !VValid(rec) {
			err = ErrBadVPacket
		} else {
			err = batch.Merge(key, rec, &WriteOptions)
		}
	}
	return
}

func (ch *Chotki) ApplyLO(id, ref ID, body []byte, batch *pebble.Batch) (err error) {
	key := FieldKey('O', id)
	value := ref.ZipBytes()
	err = batch.Merge(key, value, &WriteOptions)
	if err != nil {
		return
	}
	rest := body
	for fno := byte(1); len(rest) > 0 && fno < (1<<FNoBits); fno++ {
		lit, hlen, blen := toytlv.ProbeHeader(rest)
		if lit == 0 || lit == '-' {
			return ErrBadPacket
		}
		body = rest[hlen : hlen+blen]
		fkey := OKey(FieldID(id, fno, lit))
		switch lit {
		case 'I', 'S', 'R', 'F': // ensure correct attribution
			time, src, value := LWWparse(body)
			if value == nil {
				return ErrBadPacket
			}
			if src != id.Src() {
				body = LWWtlv(time, id.Src(), value)
			}
		default:
		}
		err = batch.Merge(fkey, body, &WriteOptions) // fixme make atomic
		if err != nil {
			return
		}
		rest = rest[hlen+blen:]
	}
	if err == nil {
		err = ch.UpdateVTree(id, id, batch)
	}
	return
}

var ErrOffsetOpId = errors.New("op id is offset")

func (ch *Chotki) ApplyE(id, r ID, body []byte, batch *pebble.Batch) (err error) {
	if id.Off() != 0 {
		return ErrOffsetOpId
	}
	xid := id
	var ref ID // FIXME offs
	var xb []byte
	rest := body
	for len(rest) > 0 {
		ref, xb, rest, err = ReadRX(rest)
		if err != nil {
			return
		}
		xid++
		key := FieldKey('O', ref)
		value := toytlv.Append(nil, 'I', xid.ZipBytes())
		value = append(value, xb...)
		err = batch.Merge(key, value, &WriteOptions)
		if err != nil {
			return
		}
	}
	vvkey := []byte{'V'}
	vvval := toytlv.Record('V', xid.ZipBytes())
	err = batch.Merge(vvkey, vvval, &WriteOptions)
	if err == nil {
		err = ch.UpdateVTree(id, r, batch)
	}
	return
}
