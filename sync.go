package chotki

import (
	"bytes"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"io"
	"sync"
)

const SyncBlockBits = 28
const SyncBlockMask = (rdx.ID(1) << SyncBlockBits) - 1

const SyncOutQueueLimit = 1 << 20

type Syncer struct {
	Host       *Chotki
	Mode       uint64
	Name       string
	peert      rdx.ID
	snap       *pebble.Snapshot
	snaplast   rdx.ID
	feedState  int
	drainState int
	oqueue     toyqueue.RecordQueue
	myvv       rdx.VV
	peervv     rdx.VV
	lock       sync.Mutex
	cond       sync.Cond
	vvit       *pebble.Iterator
	ffit       *pebble.Iterator
	vpack      []byte
	reason     error
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
	SendHandshake = iota
	SendDiff
	SendLive
	SendEOF
	SendNone
)

var SendStates = []string{
	"SendHandshake",
	"SendDiff",
	"SendLive",
	"SendEOF",
	"SendNone",
}

func (sync *Syncer) Close() error {
	sync.lock.Lock()
	if sync.Host == nil {
		sync.lock.Unlock()
		return toyqueue.ErrClosed
	}
	_ = sync.Host.RemovePacketHose(&sync.oqueue)
	sync.SetFeedState(SendNone) //fixme
	sync.lock.Unlock()
	return nil
}

func (sync *Syncer) Feed() (recs toyqueue.Records, err error) {
	switch sync.feedState {
	case SendHandshake:
		recs, err = sync.FeedHandshake()
		sync.SetFeedState(SendDiff)
	case SendDiff:
		sync.WaitDrainState(SendDiff)
		recs, err = sync.FeedBlockDiff()
		if err == io.EOF {
			recs2, _ := sync.FeedDiffVV()
			recs = append(recs, recs2...)
			if (sync.Mode & SyncLive) != 0 {
				sync.SetFeedState(SendLive)
			} else {
				sync.SetFeedState(SendEOF)
			}
			_ = sync.snap.Close()
			sync.snap = nil
			err = nil
		}
	case SendLive:
		recs, err = sync.oqueue.Feed()
		if err == toyqueue.ErrClosed {
			sync.SetFeedState(SendEOF)
			err = nil
		}
	case SendEOF:
		reason := []byte("closing")
		if sync.reason != nil {
			reason = []byte(sync.reason.Error())
		}
		recs = toyqueue.Records{toytlv.Record('B',
			toytlv.TinyRecord('T', sync.snaplast.ZipBytes()),
			reason,
		)}
		if sync.snap != nil {
			_ = sync.snap.Close()
			sync.snap = nil
		}
		sync.SetFeedState(SendNone)
	case SendNone:
		sync.WaitDrainState(SendNone) // fixme time limit
		err = io.EOF
	}
	return
}

func (sync *Syncer) EndGracefully(reason error) (err error) {
	_ = sync.Host.RemovePacketHose(&sync.oqueue)
	_ = sync.oqueue.Close()
	sync.reason = reason
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
	sync.snaplast = sync.myvv.GetID(sync.Host.src)

	sync.vpack = make([]byte, 0, 4096)
	_, sync.vpack = toytlv.OpenHeader(sync.vpack, 'V') // 5
	sync.vpack = append(sync.vpack, toytlv.Record('T', sync.snaplast.ZipBytes())...)
	v := toytlv.Record('V',
		sync.vvit.Value()) // todo use the one in handshake
	sync.vpack = append(sync.vpack, v...)

	// handshake: H(T{pro,src} M(mode) V(V{p,s}+))
	hs := toytlv.Record('H',
		toytlv.TinyRecord('T', sync.snaplast.ZipBytes()),
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
	bmark, parcel := toytlv.OpenHeader(nil, 'D')
	parcel = append(parcel, toytlv.Record('T', sync.snaplast.ZipBytes())...)
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

func (sync *Syncer) SetFeedState(state int) {
	fmt.Printf("%s feed state %s\n", sync.Name, SendStates[state])
	sync.feedState = state
}

func (sync *Syncer) SetDrainState(state int) {
	fmt.Printf("%s drain state %s\n", sync.Name, SendStates[state])
	sync.lock.Lock()
	sync.drainState = state
	if sync.cond.L == nil {
		sync.cond.L = &sync.lock
	}
	sync.cond.Broadcast()
	sync.lock.Unlock()
}

func (sync *Syncer) WaitDrainState(state int) (ds int) {
	sync.lock.Lock()
	for sync.drainState < state {
		if sync.cond.L == nil {
			sync.cond.L = &sync.lock
		}
		sync.cond.Wait()
	}
	ds = sync.drainState
	sync.lock.Unlock()
	return
}

func LastLit(recs toyqueue.Records) byte {
	if len(recs) == 0 {
		return 0
	}
	return toytlv.Lit(recs[len(recs)-1])
}

func (sync *Syncer) Drain(recs toyqueue.Records) (err error) {
	if len(recs) == 0 {
		return nil
	}
	switch sync.drainState {
	case SendHandshake:
		if len(recs) == 0 {
			return ErrBadHPacket
		}
		err = sync.DrainHandshake(recs[0:1])
		if err == nil {
			err = sync.Host.Drain(recs[0:1])
		}
		if err != nil {
			return
		}
		recs = recs[1:]
		sync.SetDrainState(SendDiff)
		if len(recs) == 0 {
			break
		}
		fallthrough
	case SendDiff:
		lit := LastLit(recs)
		if lit != 'D' && lit != 'V' {
			if lit == 'B' {
				sync.SetDrainState(SendNone)
			} else {
				sync.SetDrainState(SendLive)
			}
		}
		err = sync.Host.Drain(recs)
	case SendLive:
		lit := LastLit(recs)
		if lit == 'B' {
			sync.SetDrainState(SendNone)
		}
		err = sync.Host.Drain(recs)
	default:
		return ErrClosed
	}
	if err != nil { // todo send the error msg
		sync.drainState = SendEOF
	}
	return
}

func (sync *Syncer) DrainHandshake(recs toyqueue.Records) (err error) {
	// handshake: H(T{pro,src} M(mode) V(V{p,s}+) ...)
	var hsbody, nothing, tbody, mbody, vbody, rest []byte
	hsbody, nothing = toytlv.Take('H', recs[0])
	if len(hsbody) == 0 || len(nothing) > 0 {
		return ErrBadHPacket
	}
	tbody, rest = toytlv.Take('T', hsbody)
	if tbody == nil {
		return ErrBadHPacket
	}
	sync.peert = rdx.IDFromZipBytes(tbody)
	mbody, rest = toytlv.Take('M', rest)
	if mbody == nil {
		return ErrBadHPacket
	}
	peermode := rdx.UnzipUint64(mbody)
	_ = peermode // todo
	vbody, rest = toytlv.Take('V', rest)
	if vbody == nil {
		return ErrBadHPacket
	}
	sync.peervv = make(rdx.VV)
	err = sync.peervv.PutTLV(vbody)
	if err != nil {
		return ErrBadHPacket
	}
	_ = rest // todo
	return nil
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
