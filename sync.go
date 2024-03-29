package chotki

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
)

const VBlockBits = 28
const VBlockMask = (ID(1) << VBlockBits) - 1

func (ch *Chotki) AddPeer(peer toyqueue.FeedDrainCloser) error {
	// fixme add firehose 21 12
	q := toyqueue.RecordQueue{Limit: 1024}
	_ = ch.AddPacketHose(&q)
	snap := ch.db.NewSnapshot()
	key := [16]byte{'V'}
	vvb, clo, err := snap.Get(append(key[:1], ID0.Bytes()...))
	if err != nil {
		return err
	}
	err = peer.Drain(toyqueue.Records{vvb})
	if err != nil {
		return err
	}
	_ = clo.Close()
	// todo go ch.DoLivePeer(peer, q.Blocking())
	recvv, err := peer.Feed()
	if err != nil {
		return err // fixme close
	}
	peervvb := recvv[0]
	var peervv VV
	err = peervv.PutTLV(peervvb) // todo unenvelope
	if err != nil {
		return err
	}
	// fixme recvv[1:]
	ch.SyncPeer(peer, snap, peervv)
	return nil
}

func (ch *Chotki) SyncPeer(peer toyqueue.DrainCloser, snap *pebble.Snapshot, peervv VV) (err error) {
	vit := snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'V'},
		UpperBound: []byte{'W'},
	})
	fit := snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	})
	key0 := VKey(ID0)
	ok := vit.SeekGE(key0)
	if !ok || 0 != bytes.Compare(vit.Key(), key0) {
		return ErrBadV0Record
	}
	dbvv := make(VV)
	err = dbvv.PutTLV(vit.Value())
	if err != nil {
		return ErrBadV0Record
	}
	vpack := make([]byte, 0, 4096)
	bmark, vpack := toytlv.OpenHeader(vpack, 'V')
	v := toytlv.Record('V',
		toytlv.Record('R', ID0.ZipBytes()),
		vit.Value())
	vpack = append(vpack, v...)
	sendvv := dbvv.InterestOver(peervv)

	for vit.Next() {
		at := VKeyId(vit.Key()).ZeroOff()
		//fmt.Printf("at %s\n", at.String())
		vv := make(VV)
		err = vv.PutTLV(vit.Value())
		if err != nil {
			err = ErrBadVRecord
			break
		}
		if vv.ProgressedOver(peervv) {
			err = ch.scanObjects(fit, at, sendvv, peer)
			v := toytlv.Record('V',
				toytlv.Record('R', at.ZipBytes()),
				vit.Value()) // todo brief
			vpack = append(vpack, v...)
		}
	}

	if err == nil {
		toytlv.CloseHeader(vpack, bmark)
		err = peer.Drain(toyqueue.Records{vpack})
	}

	_ = vit.Close()
	_ = fit.Close()
	return
}

func (ch *Chotki) scanObjects(fit *pebble.Iterator, block ID, sendvv VV, peer toyqueue.DrainCloser) (err error) {
	key := OKey(block, 0)
	fit.SeekGE(key)
	bmark, parcel := toytlv.OpenHeader(nil, 'Y')
	parcel = append(parcel, toytlv.Record('R', block.ZipBytes())...)
	till := block + VBlockMask + 1
	for ; fit.Valid(); fit.Next() {
		id, rdt := OKeyIdRdt(fit.Key())
		if id == BadId || id >= till {
			break
		}
		lim, ok := sendvv[id.Src()]
		if ok && (id.Pro() > lim || lim == 0) {
			parcel = append(parcel, toytlv.Record('F', ZipUint64(uint64(id-block)))...)
			parcel = append(parcel, toytlv.Record(rdt, fit.Value())...)
			continue
		}
		var diff []byte
		switch rdt {
		case 'A':
			diff = nil
		case 'I':
			diff = Idiff(fit.Value(), sendvv)
		case 'S':
			diff = Sdiff(fit.Value(), sendvv)
		default:
			diff = fit.Value()
		}
		if len(diff) != 0 {
			parcel = append(parcel, toytlv.Record('F', ZipUint64(uint64(id-block)))...)
			parcel = append(parcel, toytlv.Record(rdt, diff)...)
		}
	}
	if err == nil {
		toytlv.CloseHeader(parcel, bmark)
		err = peer.Drain(toyqueue.Records{parcel})
	}
	return
}

func ParseVPack(vpack []byte) (vvs map[ID]VV, err error) {
	lit, body, rest := toytlv.TakeAny(vpack)
	if lit != 'V' || len(rest) > 0 {
		return nil, ErrBadVPacket
	}
	vvs = make(map[ID]VV)
	vrest := body
	for len(vrest) > 0 {
		var rv, r, v []byte
		lit, rv, vrest = toytlv.TakeAny(vrest)
		if lit != 'V' {
			return nil, ErrBadVPacket
		}
		lit, r, v := toytlv.TakeAny(rv)
		if lit != 'R' {
			return nil, ErrBadVPacket
		}
		syncvv := make(VV)
		err = syncvv.PutTLV(v)
		if err != nil {
			return nil, ErrBadVPacket
		}
		id := IDFromZipBytes(r)
		vvs[id] = syncvv
	}
	return
}

type Baker struct {
	ch        *Chotki
	sout, sin int
	batch     *pebble.Batch
	delta     toyqueue.RecordQueue
	inq       toyqueue.Drainer //nolint:golint,unused
	outq      toyqueue.RecordQueue
}

const (
	SendVV = iota
	SendDelta
	SendCurrent
	SendEOF
	SendNothing
)

func (b *Baker) Feed() (recs toyqueue.Records, err error) {
	switch b.sout {
	case SendVV:
	case SendDelta:
		recs, err = b.delta.Feed()
		if err != nil {
			b.sout++
			err = nil
		}
	case SendCurrent:
		recs, err = b.outq.Feed()
		if err != nil {
			b.sout++
			err = nil
		}
	case SendEOF:
		_ = b.outq.Close()
		b.sout++
	case SendNothing:
		err = toyqueue.ErrClosed
	}
	return
}

func (b *Baker) Drain(recs toyqueue.Records) (err error) {
	switch b.sin {
	case SendVV:
		vv := make(VV)
		vvr := recs[0]
		recs = recs[1:]
		err = vv.PutTLV(vvr)
		if err != nil {
			break
		}
		b.outq.Limit = 1024
		b.delta.Limit = 1024
		err = b.ch.AddPacketHose(&b.outq)
		if err != nil {
			break
		}
		snap := b.ch.db.NewSnapshot()
		go b.ch.SyncPeer(&b.delta, snap, vv)
		b.batch = b.ch.db.NewBatch()
		b.sin++
		if len(recs) == 0 {
			break
		}
		fallthrough
	case SendDelta:
		for len(recs) > 0 {
			rec := recs[0]
			recs = recs[1:]
			lit, body, rest := toytlv.TakeAny(rec)
			if len(rest) > 0 {
				err = ErrBadPacket
				break
			}
			switch lit {
			case 'Y':
				err = b.ch.ApplyY(ID0, ID0, body, b.batch)
			case 'M':
				wo := pebble.WriteOptions{Sync: true}
				err = b.ch.db.Apply(b.batch, &wo)
				b.sin++
				if err == nil && len(recs) > 0 {
					err = b.Drain(recs)
				}
			default:
				err = ErrBadPacket
			}
		}
	case SendCurrent:
		err = b.ch.Drain(recs)
		if err != nil {
			b.sin++
		}
	case SendEOF:
		return toyqueue.ErrClosed
	case SendNothing:
		return toyqueue.ErrClosed
	}
	// TODO: close things
	// if err != nil { }
	return
}

var ErrNotImplemented = errors.New("not implemented")

func (b *Baker) Close() error {
	return ErrNotImplemented
}
