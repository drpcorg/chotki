package chotki

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

func (cho *Chotki) UpdateVTree(id, ref rdx.ID, pb *pebble.Batch) (err error) {
	v := protocol.Record('V', id.ZipBytes())
	err = pb.Merge(VKey(ref), v, &pebbleWriteOptions)
	if err == nil {
		err = pb.Merge(VKey(rdx.ID0), v, &pebbleWriteOptions)
	}
	return
}

func (cho *Chotki) ApplyD(id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	rest := body
	var rdt byte
	for len(rest) > 0 && err == nil {
		var dzip, bare []byte
		dzip, rest = protocol.Take('F', rest)
		d := rdx.UnzipUint64(dzip)
		at := ref + rdx.ID(d) // fixme
		rdt, bare, rest = protocol.TakeAny(rest)
		err = batch.Merge(OKey(at, rdt), bare, &pebbleWriteOptions)
	}
	return
}

func (cho *Chotki) ApplyH(id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	_, rest := protocol.Take('M', body)
	var vbody []byte
	vbody, _ = protocol.Take('V', rest)
	err = batch.Merge(VKey(rdx.ID0), vbody, &pebbleWriteOptions)
	return
}

func (cho *Chotki) ApplyV(id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	rest := body
	for len(rest) > 0 {
		var rec, idb []byte
		rec, rest = protocol.Take('V', rest)
		idb, rec = protocol.Take('R', rec)
		id := rdx.IDFromZipBytes(idb)
		key := VKey(id)
		if !rdx.VValid(rec) {
			err = ErrBadVPacket
		} else {
			err = batch.Merge(key, rec, &pebbleWriteOptions)
		}
	}
	return
}

func (cho *Chotki) ApplyC(id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	err = batch.Merge(
		OKey(id, 'C'),
		body,
		&pebbleWriteOptions)
	if err == nil {
		err = cho.UpdateVTree(id, ref, batch)
	}
	return
}

func (cho *Chotki) ApplyOY(lot byte, id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	err = batch.Merge(
		OKey(id, lot),
		ref.ZipBytes(),
		&pebbleWriteOptions)
	rest := body
	var fid rdx.ID
	for fno := rdx.ID(1); len(rest) > 0 && err == nil; fno++ {
		lit, hlen, blen := protocol.ProbeHeader(rest)
		if lit == 0 || lit == '-' {
			return rdx.ErrBadPacket
		}
		var bare, rebar []byte
		rlen := hlen + blen
		if len(rest) < rlen {
			return ErrBadOPacket
		}
		bare = rest[hlen:rlen]
		fid = id + fno
		fkey := OKey(fid, lit)
		switch lit {
		case 'F', 'I', 'R', 'S', 'T':
			rebar, err = rdx.SetSourceFIRST(bare, id.Src())
		case 'E', 'L', 'M':
			rebar, err = rdx.MelReSource(bare, id.Src())
		default:
			rebar = bare
		}
		if err != nil {
			break
		}
		err = batch.Merge(
			fkey,
			rebar,
			&pebbleWriteOptions)
		rest = rest[rlen:]
	}
	if err == nil {
		err = cho.UpdateVTree(fid, id, batch)
	}
	return
}

var ErrOffsetOpId = errors.New("op id is offset")

func (cho *Chotki) ApplyE(id, r rdx.ID, body []byte, batch *pebble.Batch, calls *[]CallHook) (err error) {
	if id.Off() != 0 || r.Off() != 0 {
		return ErrOffsetOpId
	}
	rest := body
	for len(rest) > 0 && err == nil {
		var fint, bare, rebar []byte
		var lit byte
		fint, rest = protocol.Take('F', rest)
		field := rdx.UnzipUint64(fint)
		if field > uint64(rdx.OffMask) {
			return ErrBadEPacket
		}
		lit, bare, rest = protocol.TakeAny(rest)
		switch lit {
		case 'F', 'I', 'R', 'S', 'T':
			rebar, err = rdx.SetSourceFIRST(bare, id.Src())
		case 'E', 'L', 'M':
			rebar, err = rdx.MelReSource(bare, id.Src())
		default:
			rebar = bare
		}
		if err != nil {
			break
		}
		fid := r + rdx.ID(field)
		fkey := OKey(fid, lit)
		err = batch.Merge(
			fkey,
			rebar,
			&pebbleWriteOptions)
		hook, ok := cho.hooks.Load(fid)
		if ok {
			for _, h := range hook {
				(*calls) = append((*calls), CallHook{h, fid})
			}
		}
	}
	if err == nil {
		err = cho.UpdateVTree(id, r, batch)
	}
	return
}
