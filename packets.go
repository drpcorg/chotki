package chotki

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/indexes"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

// Updates the Vkey0, which stores version vectors for different replicas.
// But also updates the corresponding block (check replication protocol description) version vector.
// Version vector is a map: replica src id -> latest seen rdx.ID
func (cho *Chotki) UpdateVTree(id, ref rdx.ID, pb *pebble.Batch) (err error) {
	// id itself contains src and id, the final value will be done through merge.
	v := protocol.Record('V', id.ZipBytes())
	// updating block version vector
	err = pb.Merge(host.VKey(ref), v, cho.opts.PebbleWriteOptions)
	if err == nil {
		// updating global version vector
		err = pb.Merge(host.VKey0, v, cho.opts.PebbleWriteOptions)
	}
	return
}

// During the diff sync it handles the 'D' packets which most of the time contains a single block (look at the replication protocol description).
// It does not immediately apply the changes to DB, instead using a batch.
// The batch will be applied when we finish the diffsync, when we receive the 'V' packet.
func (cho *Chotki) ApplyD(id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	rest := body
	var rdt byte
	for len(rest) > 0 && err == nil {
		var dzip, bare []byte
		// this is id, but its stored as an offset of ref to save some bytes
		dzip, rest = protocol.Take('F', rest)
		// It also stored as zigzagged
		d := rdx.UnzipUint64(dzip)
		// we now restore original id
		at := ref.ProPlus(d)
		rdt, bare, rest = protocol.TakeAny(rest)
		// we updated some classes, so dropping cache
		if rdt == 'C' {
			cho.types.Clear()
		}
		err = batch.Merge(host.OKey(at, rdt), bare, cho.opts.PebbleWriteOptions)
		// adding full scan index if the object was created
		if err == nil && rdt == 'O' {
			cid := rdx.IDFromZipBytes(bare)
			err = cho.IndexManager.AddFullScanIndex(cid, at, batch)
		} else {
			// check if we need add other types of indexes
			err = cho.IndexManager.OnFieldUpdate(rdt, at, rdx.BadId, bare, batch)
		}
	}
	return
}

// During the diff sync it handles the 'H' packets which contains the version vector of
// other replica. So we put their version vector into the batch to update ours after diff sync.
func (cho *Chotki) ApplyH(id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	_, rest := protocol.Take('M', body)
	var vbody []byte
	vbody, _ = protocol.Take('V', rest)
	err = batch.Merge(host.VKey0, vbody, cho.opts.PebbleWriteOptions)
	return
}

// During the diff sync it handles the 'V' packets. This packet effectively completes the diffy sync.
// It contains the version vector of the blocks we synced during the diff sync.
// We put then in the batch to update our blocks version vectors.
func (cho *Chotki) ApplyV(id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	rest := body
	for len(rest) > 0 {
		var rec, idb []byte
		// take block version vector
		rec, rest = protocol.Take('V', rest)
		// take block id
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

// Handles 'C' packets which can occur from applying local changes or live sync.
// Creates or updates class definition.
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
		tasks, err = cho.IndexManager.HandleClassUpdate(id, cid, body)
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

// Handles 'O' packets which can occur from applying local changes or live sync.
// Creates objects 'O'. 'Y' are special kind of objects, unused at the moment.
// Typically expects an 'O' package:
// 0 — 'O' record that contains class rdx.ID
// 1 - ... - class fields
// First it creates 'O' field.
// Then it goes through the rest of the fields encoded as TLV. The only transformation it does:
// it sets the current replica src id for FIRST/MEL types, because historically they are not set
// when creating those fields (for convinience?)
func (cho *Chotki) ApplyOY(lot byte, id, ref rdx.ID, body []byte, batch *pebble.Batch) (err error) {
	// creating 'O' field, ref is class rdx.ID
	err = batch.Merge(
		host.OKey(id, lot),
		ref.ZipBytes(),
		cho.opts.PebbleWriteOptions)
	rest := body
	var fid rdx.ID
	// 0 is 'O', so we start from 1
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
		// setting current replica src id for FIRST/MEL types
		switch lit {
		case 'F', 'I', 'R', 'S', 'T':
			rebar, err = rdx.SetSourceFIRST(bare, id.Src())
		case 'E', 'L', 'M':
			rebar, err = rdx.MelReSource(bare, id.Src())
		default:
			// for NZ types and mauy be others, we expect that the src id is already set
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
			err = cho.IndexManager.OnFieldUpdate(lit, fid, ref, rebar, batch)
		}
	}
	if err == nil {
		err = cho.UpdateVTree(fid, id, batch)
	}
	if err == nil && lot == 'O' {
		err = cho.IndexManager.AddFullScanIndex(ref, id, batch)
	}
	return
}

var ErrOffsetOpId = errors.New("op id is offset")

// Handles 'E' packets which can occur from applying local changes or live sync.
// Edits obkject fields. Unlike ApplyOY, it does not assume that we update whole object,
// as we can update individual fields.
// It also sets the current replica src id for FIRST/MEL types. Otherwise its just merges bytes into the batch.
func (cho *Chotki) ApplyE(id, r rdx.ID, body []byte, batch *pebble.Batch, calls *[]CallHook) (err error) {
	// we either supply id of the object (0 offset) or ref should be an id of the object
	if id.Off() != 0 || r.Off() != 0 {
		return ErrOffsetOpId
	}
	rest := body
	for len(rest) > 0 && err == nil {
		var fint, bare, rebar []byte
		var lit byte
		// field always starts with 'F', which encodes field offset
		fint, rest = protocol.Take('F', rest)
		if fint == nil {
			return ErrBadEPacket
		}
		field := rdx.UnzipUint64(fint)
		if field > uint64(rdx.OffMask) {
			return ErrBadEPacket
		}
		lit, bare, rest = protocol.TakeAny(rest)
		// setting current replica src id for FIRST/MEL types
		switch lit {
		case 'F', 'I', 'R', 'S', 'T':
			rebar, err = rdx.SetSourceFIRST(bare, id.Src())
		case 'E', 'L', 'M':
			rebar, err = rdx.MelReSource(bare, id.Src())
		default:
			// for NZ types and mauy be others, we expect that the src id is already set
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
			err = cho.IndexManager.OnFieldUpdate(lit, fid, rdx.BadId, rebar, batch)
		}
		// hooks are used for REPL sometimes (or where used), otherwise unused
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
