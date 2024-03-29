package chotki

import (
	"bytes"
	"errors"
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"io"
	"sync"
	"time"
)

const SyncBlockBits = 28
const SyncBlockMask = (rdx.ID(1) << SyncBlockBits) - 1

const SyncOutQueueLimit = 1 << 20

type Syncer struct {
	Host   *Chotki
	Mode   uint64
	peert  rdx.ID
	snap   *pebble.Snapshot
	ostate int
	istate int
	oqueue toyqueue.RecordQueue
	myvv   rdx.VV
	peervv rdx.VV
	lock   sync.Mutex
	vvit   *pebble.Iterator
	ffit   *pebble.Iterator
	vpack  []byte
}

const (
	SyncRead  = 1
	SyncWrite = 2
	SyncLive  = 4
	SyncRW    = SyncRead | SyncWrite
	SyncRL    = SyncRead | SyncLive
	SyncRWL   = SyncRead | SyncWrite | SyncLive
)

const (
	SendVV = iota
	SendDelta
	SendCurrent
	SendEOF
	SendNothing
)

func (sync *Syncer) Close() error {
	sync.lock.Lock()
	if sync.Host == nil {
		sync.lock.Unlock()
		return toyqueue.ErrClosed
	}
	_ = sync.Host.RemovePacketHose(&sync.oqueue)
	if sync.snap != nil {
		_ = sync.snap.Close()
		sync.snap = nil
	}
	sync.ostate = SendNothing
	sync.lock.Unlock()
	return nil
}

func (sync *Syncer) Feed() (recs toyqueue.Records, err error) {
	switch sync.ostate {
	case SendVV:
		sync.ostate++
		recs, err = sync.FeedHandshake()
	case SendDelta:
		for sync.istate == SendVV && sync.Host != nil {
			time.Sleep(time.Millisecond)
		}
		recs, err = sync.FeedBlockDiff()
		if err == io.EOF {
			recs2, _ := sync.FeedDiffVV()
			recs = append(recs, recs2...)
			if (sync.Mode & SyncLive) != 0 {
				sync.ostate++
			} else {
				sync.ostate = SendEOF
			}
			err = nil
		}
	case SendCurrent:
		recs, err = sync.FeedUpdates()
	case SendEOF:
		_ = sync.oqueue.Close()
		sync.ostate++
		err = io.EOF
	case SendNothing:
		err = toyqueue.ErrClosed
	}
	return
}

func (sync *Syncer) FeedHandshake() (vv toyqueue.Records, err error) {
	sync.oqueue.Limit = SyncOutQueueLimit
	err = sync.Host.AddPacketHose(&sync.oqueue)
	if err != nil {
		_ = sync.Close()
		return
	}

	sync.snap = sync.Host.db.NewSnapshot()
	sync.vvit = sync.snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'V'},
		UpperBound: []byte{'W'},
	})
	sync.ffit = sync.snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	})

	key0 := VKey(rdx.ID0)
	ok := sync.vvit.SeekGE(key0)
	if !ok || 0 != bytes.Compare(sync.vvit.Key(), key0) {
		return nil, rdx.ErrBadV0Record
	}
	sync.myvv = make(rdx.VV)
	err = sync.myvv.PutTLV(sync.vvit.Value())
	if err != nil {
		return nil, err
	}

	sync.vpack = make([]byte, 0, 4096)
	_, sync.vpack = toytlv.OpenHeader(sync.vpack, 'V') // 5
	v := toytlv.Record('V',
		toytlv.Record('R', rdx.ID0.ZipBytes()),
		sync.vvit.Value()) // todo use the one in handshake
	sync.vpack = append(sync.vpack, v...)

	// handshake: H(T{pro,src} M(mode) V(V{p,s}+))
	hs := toytlv.Record('H',
		toytlv.TinyRecord('T', sync.Host.last.ZipBytes()),
		toytlv.TinyRecord('M', rdx.ZipUint64(sync.Mode)),
		toytlv.Record('V', sync.vvit.Value()),
	)

	return toyqueue.Records{hs}, nil
}

func (sync *Syncer) FeedBlockDiff() (diff toyqueue.Records, err error) {
	if !sync.vvit.Next() {
		return nil, io.EOF
	}
	vv := make(rdx.VV)
	err = vv.PutTLV(sync.vvit.Value())
	if err != nil {
		return nil, rdx.ErrBadVRecord
	}
	sendvv := make(rdx.VV)
	// check for any changes
	hasChanges := false // fixme up & repeat
	for src, pro := range vv {
		peerpro, ok := sync.peervv[src]
		if !ok || pro > peerpro {
			sendvv[src] = peerpro
			hasChanges = true
		}
	}
	if !hasChanges {
		return toyqueue.Records{}, nil
	}
	block := VKeyId(sync.vvit.Key()).ZeroOff()
	key := OKey(block, 0)
	sync.ffit.SeekGE(key)
	bmark, parcel := toytlv.OpenHeader(nil, 'Y')
	parcel = append(parcel, toytlv.Record('R', block.ZipBytes())...)
	till := block + SyncBlockMask + 1
	for ; sync.ffit.Valid(); sync.ffit.Next() {
		id, rdt := OKeyIdRdt(sync.ffit.Key())
		if id == rdx.BadId || id >= till {
			break
		}
		lim, ok := sendvv[id.Src()]
		if ok && (id.Pro() > lim || lim == 0) {
			parcel = append(parcel, toytlv.Record('F', rdx.ZipUint64(uint64(id-block)))...)
			parcel = append(parcel, toytlv.Record(rdt, sync.ffit.Value())...)
			continue
		}
		var diff []byte
		switch rdt {
		case 'A':
			diff = nil
		case 'I':
			diff = rdx.Idiff(sync.ffit.Value(), sendvv)
		case 'S':
			diff = rdx.Sdiff(sync.ffit.Value(), sendvv)
		default:
			diff = sync.ffit.Value()
		}
		if len(diff) != 0 {
			parcel = append(parcel, toytlv.Record('F', rdx.ZipUint64(uint64(id-block)))...)
			parcel = append(parcel, toytlv.Record(rdt, diff)...)
		}
	}
	toytlv.CloseHeader(parcel, bmark)
	v := toytlv.Record('V',
		toytlv.Record('R', block.ZipBytes()),
		sync.vvit.Value()) // todo brief
	sync.vpack = append(sync.vpack, v...)
	return toyqueue.Records{parcel}, err
}

func (sync *Syncer) FeedDiffVV() (vv toyqueue.Records, err error) {
	toytlv.CloseHeader(sync.vpack, 5)
	vv = append(vv, sync.vpack)
	sync.vpack = nil
	_ = sync.ffit.Close()
	sync.ffit = nil
	_ = sync.vvit.Close()
	sync.vvit = nil
	return
}

func (sync *Syncer) FeedUpdates() (vv toyqueue.Records, err error) {
	return sync.oqueue.Feed()
}

func (sync *Syncer) Drain(recs toyqueue.Records) (err error) {
	switch sync.istate {
	case SendVV:
		if len(recs) == 0 {
			return ErrBadHPacket
		}
		err = sync.DrainHandshake(recs[0:1])
		recs = recs[1:]
		sync.istate++
		if len(recs) == 0 {
			break
		}
		fallthrough
	case SendDelta:
		err = sync.Host.Drain(recs)
	case SendCurrent:
		err = sync.Host.Drain(recs)
	case SendEOF:
		return toyqueue.ErrClosed
	case SendNothing:
		return toyqueue.ErrClosed
	}
	if err != nil { // todo send the error msg
		sync.istate = SendEOF
	}
	return
}

func (sync *Syncer) DrainHandshake(recs toyqueue.Records) (err error) {
	// handshake: H(T{pro,src} M(mode) V(V{p,s}+) ...)
	hsbody, nothing := toytlv.Take('H', recs[0])
	if len(hsbody) == 0 || len(nothing) > 0 {
		return ErrBadHPacket
	}
	tbody, rest := toytlv.Take('T', hsbody)
	if tbody == nil {
		return ErrBadHPacket
	}
	sync.peert = rdx.IDFromZipBytes(tbody)
	mbody, rest := toytlv.Take('M', rest)
	if mbody == nil {
		return ErrBadHPacket
	}
	peermode := rdx.UnzipUint64(mbody)
	_ = peermode // todo
	vbody, rest := toytlv.Take('V', rest)
	if vbody == nil {
		return ErrBadHPacket
	}
	sync.peervv = make(rdx.VV)
	err = sync.peervv.PutTLV(vbody)
	if err != nil {
		return ErrBadHPacket
	}
	return nil
}

// Usage:
// 1. FeedHandshake to the peer
// 2. DrainPeerVV
// 3. repeat FeedBlock
// 4. FeedBlockVV
// 5. Done
type Differ struct {
	snap   *pebble.Snapshot
	peervv rdx.VV
	vpack  []byte
}

func (diff *Differ) DrainPeerVV(vvrec toyqueue.Records) (err error) {
	diff.peervv = make(rdx.VV)
	if len(vvrec) != 1 {
		return ErrBadVPacket // todo
	}
	err = diff.peervv.PutTLV(vvrec[0])
	return
}

func ParseVPack(vpack []byte) (vvs map[rdx.ID]rdx.VV, err error) {
	lit, body, rest := toytlv.TakeAny(vpack)
	if lit != 'V' || len(rest) > 0 {
		return nil, ErrBadVPacket
	}
	vvs = make(map[rdx.ID]rdx.VV)
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
		syncvv := make(rdx.VV)
		err = syncvv.PutTLV(v)
		if err != nil {
			return nil, ErrBadVPacket
		}
		id := rdx.IDFromZipBytes(r)
		vvs[id] = syncvv
	}
	return
}

var ErrNotImplemented = errors.New("not implemented")
