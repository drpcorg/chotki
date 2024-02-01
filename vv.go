package main

import (
	"errors"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"sync"
)

type VV map[uint32]uint32

func (vv VV) Get(orig uint32) (val uint32) {
	val, _ = vv[orig]
	return
}

func (vv VV) Put(orig, val uint32) {
	vv[orig] = val
}

var ErrSeen = errors.New("previously seen id")
var ErrGap = errors.New("id sequence gap")

func (vv VV) PutSeq(orig, val uint32) error {
	has, _ := vv[orig]
	if has >= val {
		return ErrSeen
	} else if has+1 < val {
		return ErrGap
	} else {
		vv[orig] = val
		return nil
	}
}

// TLV Vv record
func (vv VV) Bytes() (ret []byte) {
	bm, ret := toytlv.OpenHeader(ret, 'V')
	for orig, seq := range vv {
		ret = toytlv.Append(ret, 'V', ZipUint64Pair(uint64(seq), uint64(orig)))
	}
	toytlv.CloseHeader(ret, bm)
	return
}

const (
	VvSeen = -1
	VvNext = 0
	VvGap  = 1
)

func (vv VV) Next(id ID) int {
	return vv.Next2(id.SeqOff(), id.Src())
}

// returns -1 for already seen, 1 for causal gaps, 0 for OK
func (vv VV) Next2(seqoff, src uint32) int {
	val, _ := vv[src]
	if val >= seqoff {
		return VvSeen
	}
	if val+1 < seqoff { // FIXME :)
		return VvGap
	}
	vv[src] = seqoff
	return VvNext
}

// .{I .*}
// Ignores already-seen; returns ErrGap on sequence gaps
func (vv VV) Filter(batch toyqueue.Records) (new_batch toyqueue.Records, err error) {
	new_batch = make(toyqueue.Records, 0, len(batch))
	for _, pack := range batch {
		seq, src := PacketID(pack)
		val, _ := vv[src]
		if val >= seq {
			continue // ignore
		}
		if val+1 < seq {
			return nil, ErrGap
		}
		new_batch = append(new_batch, pack)
	}
	return new_batch, nil
}

var ErrBadVRecord = errors.New("bad Vv record")

// consumes: Vv record
func (vv VV) LoadBytes(rec []byte) error {
	lit, body, rest, err := toytlv.TakeAnyWary(rec)
	if err != nil {
		return err
	}
	if len(rest) != 0 || lit != 'V' {
		return ErrBadVRecord
	}
	for len(body) > 0 {
		var val []byte
		val, body, err = toytlv.TakeWary('V', body)
		if err != nil {
			return err
		}
		big, lil := UnzipUint64Pair(val)
		seq, src := uint32(big), uint32(lil)
		have := vv[src]
		if seq > have {
			vv[src] = seq
		}
	}
	return nil
}

func (vv VV) Covers(bb VV) bool {
	for src, bseq := range bb {
		seq := vv[src]
		if bseq > seq {
			return false
		}
	}
	return true
}

func (vv VV) GetID(orig uint32) ID {
	seq := vv.Get(orig)
	return MakeID(orig, seq, 0)
}

type VVFeeder struct {
	vv   VV
	lock sync.Locker
}

func (vvf VVFeeder) Feed() (recs toyqueue.Records, err error) {
	if vvf.lock != nil {
		vvf.lock.Lock()
	}
	recs = toyqueue.Records{vvf.vv.Bytes()}
	if vvf.lock != nil {
		vvf.lock.Unlock()
	}
	return
}
