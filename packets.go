package main

import (
	"errors"
	"github.com/cockroachdb/pebble"
	"github.com/learn-decentralized-systems/toytlv"
)

func (ch *Chotki) UpdateVTree(id, ref ID, pb *pebble.Batch) (err error) {
	v := toytlv.Record('V', id.ZipBytes())
	err = pb.Merge(VKey(ref), v, &WriteOptions)
	if err == nil {
		err = pb.Merge(VKey(ID0), v, &WriteOptions)
	}
	return
}

func (ch *Chotki) ApplyY(id, ref ID, body []byte, batch *pebble.Batch) (err error) {
	// see Chotki.SyncPeer()
	rest := body
	var rdt byte
	for len(rest) > 0 && err == nil {
		var dzip, bare []byte
		dzip, rest = toytlv.Take('F', rest)
		d := UnzipUint64(dzip)
		at := ref + ID(d) // fixme
		rdt, bare, rest = toytlv.TakeAny(rest)
		err = batch.Merge(OKey(at, rdt), bare, &WriteOptions)
	}
	return
}

func (ch *Chotki) ApplyV(id, ref ID, body []byte, batch *pebble.Batch) (err error) {
	rest := body
	for len(rest) > 0 {
		var rec, idb []byte
		rec, rest = toytlv.Take('V', rest)
		idb, rec = toytlv.Take('R', rec)
		id := IDFromZipBytes(idb)
		key := VKey(id)
		if !VValid(rec) {
			err = ErrBadVPacket
		} else {
			err = batch.Merge(key, rec, &WriteOptions)
		}
	}
	return
}

func (ch *Chotki) ApplyLOT(id, ref ID, body []byte, batch *pebble.Batch) (err error) {
	err = batch.Merge(
		OKey(id, 'A'),
		ref.ZipBytes(),
		&WriteOptions)
	rest := body
	var fid ID
	for fno := ID(1); len(rest) > 0 && err == nil; fno++ {
		lit, hlen, blen := toytlv.ProbeHeader(rest)
		if lit == 0 || lit == '-' {
			return ErrBadPacket
		}
		body = rest[hlen : hlen+blen]
		fid = id + fno
		fkey := OKey(fid, lit)
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

func (ch *Chotki) ApplyE(id, r ID, body []byte, batch *pebble.Batch) (err error) {
	if id.Off() != 0 || r.Off() != 0 {
		return ErrOffsetOpId
	}
	rest := body
	for len(rest) > 0 && err == nil {
		var fint, bare []byte
		var lit byte
		fint, rest = toytlv.Take('F', rest)
		field := UnzipUint64(fint)
		if field > uint64(OffMask) {
			return ErrBadEPacket
		}
		lit, bare, rest = toytlv.TakeAny(rest)
		fkey := OKey(r+ID(field), lit)
		err = batch.Merge(
			fkey,
			bare,
			&WriteOptions)
	}
	if err == nil {
		err = ch.UpdateVTree(id, r, batch)
	}
	return
}
