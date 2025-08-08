package chotki

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/indexes"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

func (cho *Chotki) UpdateVTree(id, ref rdx.ID, pb *pebble.Batch) (err error) {
	v := protocol.Record('V', id.ZipBytes())
	err = pb.Merge(host.VKey(ref), v, cho.opts.PebbleWriteOptions)
	if err == nil {
		err = pb.Merge(host.VKey0, v, cho.opts.PebbleWriteOptions)
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
		at := ref.ProPlus(d)
		rdt, bare, rest = protocol.TakeAny(rest)
		// clean cache after class update
		if rdt == 'C' {
			cho.types.Clear()
		}
		err = batch.Merge(host.OKey(at, rdt), bare, cho.opts.PebbleWriteOptions)
		if err == nil && rdt == 'O' {
			cid := rdx.IDFromZipBytes(bare)
			err = cho.indexManager.AddFullScanIndex(cid, at, batch)
		} else {
			err = cho.indexManager.OnFieldUpdate(rdt, at, rdx.BadId, bare, batch)
		}
	}
	return
}

func (cho *Chotki) ApplyH(id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	_, rest := protocol.Take('M', body)
	var vbody []byte
	vbody, _ = protocol.Take('V', rest)
	err = batch.Merge(host.VKey0, vbody, cho.opts.PebbleWriteOptions)
	return
}

func (cho *Chotki) ApplyV(id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	rest := body
	for len(rest) > 0 {
		var rec, idb []byte
		rec, rest = protocol.Take('V', rest)
		idb, rec = protocol.Take('R', rec)
		id := rdx.IDFromZipBytes(idb)
		key := host.VKey(id)
		if !rdx.VValid(rec) {
			err = ErrBadVPacket
		} else {
			err = batch.Merge(key, rec, cho.opts.PebbleWriteOptions)
		}
	}
	return
}

// Classes are special objects. They are stored in separate key range in pebble.
// They also have more simplified behaviour in create/update scenarios: they are just created/replaced as is.
// All classes are created with rid = rdx.ID0, if you pass other ref, it should be real ref of what you want to edit.
func (cho *Chotki) ApplyC(id, ref rdx.ID, body []byte, batch *pebble.Batch, calls *[]CallHook) (err error) {
	cid := id
	// editing class
	if ref != rdx.ID0 {
		cid = ref
	}
	err = batch.Merge(
		host.OKey(cid, 'C'),
		body,
		cho.opts.PebbleWriteOptions)
	if err == nil {
		err = cho.UpdateVTree(id, cid, batch)
	}
	if err == nil {
		var tasks []indexes.ReindexTask
		tasks, err = cho.indexManager.HandleClassUpdate(id, cid, body)
		if err == nil {
			for _, task := range tasks {
				err = batch.Merge(task.Key(), task.Value(), cho.opts.PebbleWriteOptions)
				if err != nil {
					break
				}
			}
		}
	}
	return
}

func (cho *Chotki) ApplyOY(lot byte, id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	err = batch.Merge(
		host.OKey(id, lot),
		ref.ZipBytes(),
		cho.opts.PebbleWriteOptions)
	rest := body
	var fid rdx.ID
	for fno := 1; len(rest) > 0 && err == nil; fno++ {
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
		fid = id.ToOff(uint64(fno))
		fkey := host.OKey(fid, lit)
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
			cho.opts.PebbleWriteOptions)
		rest = rest[rlen:]
		if err == nil {
			err = cho.indexManager.OnFieldUpdate(lit, fid, ref, rebar, batch)
		}
	}
	if err == nil {
		err = cho.UpdateVTree(fid, id, batch)
	}
	if err == nil && lot == 'O' {
		err = cho.indexManager.AddFullScanIndex(ref, id, batch)
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
		if fint == nil {
			return ErrBadEPacket
		}
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
		fid := r.ToOff(field)
		fkey := host.OKey(fid, lit)
		err = batch.Merge(
			fkey,
			rebar,
			cho.opts.PebbleWriteOptions)

		if err == nil {
			err = cho.indexManager.OnFieldUpdate(lit, fid, rdx.BadId, rebar, batch)
		}
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
