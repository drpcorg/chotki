package main

import (
	"errors"
	"github.com/cockroachdb/pebble"
	"github.com/learn-decentralized-systems/toytlv"
)

func FieldID(oid id64, fno byte, rdt byte) id64 {
	oid &= id64(^OffMask)
	oid |= id64(rdt - 'A')
	oid |= id64(fno) << RdtBits
	return oid
}

func (ch *Chotki) UpdateVTree(id, ref id64, pb *pebble.Batch) (err error) {
	v := toytlv.Record('V', id.ZipBytes())
	err = pb.Merge(VKey(ref), v, &WriteOptions)
	if err == nil {
		err = pb.Merge(VKey(ID0), v, &WriteOptions)
	}
	return
}

func (ch *Chotki) ApplyY(id, ref id64, body []byte, batch *pebble.Batch) (err error) {
	// see Chotki.SyncPeer()
	rest := body
	for len(rest) > 0 && err == nil {
		var dzip, tlv []byte
		dzip, rest = toytlv.Take('F', rest)
		d := UnzipUint64(dzip)
		at := ref + id64(d) // fixme
		_, tlv, rest = toytlv.TakeAny(rest)
		err = batch.Merge(OKey(at), tlv, &WriteOptions)
	}
	return
}

func (ch *Chotki) ApplyV(id, ref id64, body []byte, batch *pebble.Batch) (err error) {
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

func (ch *Chotki) ApplyLO(id, ref id64, body []byte, batch *pebble.Batch) (err error) {
	err = batch.Merge(
		FieldKey('O', id),
		ref.ZipBytes(),
		&WriteOptions)
	rest := body
	var fid id64
	for fno := byte(1); len(rest) > 0 && fno < (1<<FNoBits) && err == nil; fno++ {
		lit, hlen, blen := toytlv.ProbeHeader(rest)
		if lit == 0 || lit == '-' {
			return ErrBadPacket
		}
		body = rest[hlen : hlen+blen]
		fid = FieldID(id, fno, lit)
		fkey := OKey(fid)
		switch lit {
		case 'I', 'S', 'R', 'F':
			time, src, value := LWWparse(body)
			if value == nil {
				return ErrBadPacket
			}
			if src != id.Src() { // ensure correct attribution
				body = LWWtlv(time, id.Src(), value)
			}
		default:
		}
		err = batch.Merge(
			fkey,
			body,
			&WriteOptions)
		rest = rest[hlen+blen:]
	}
	if err == nil {
		err = ch.UpdateVTree(fid, id, batch)
	}
	return
}

var ErrOffsetOpId = errors.New("op id is offset")

func (ch *Chotki) ApplyE(id, r id64, body []byte, batch *pebble.Batch) (err error) {
	if id.Off() != 0 {
		return ErrOffsetOpId
	}
	xid := id
	var ref id64 // FIXME offs
	var xb []byte
	rest := body
	for len(rest) > 0 && err == nil {
		ref, xb, rest, err = ReadRX(rest)
		if err != nil {
			break
		}
		xid++
		key := FieldKey('O', ref)
		value := toytlv.Append(nil, 'I', xid.ZipBytes())
		value = append(value, xb...)
		err = batch.Merge(key, value, &WriteOptions)
	}
	if err == nil {
		err = ch.UpdateVTree(id, r, batch)
	}
	return
}
