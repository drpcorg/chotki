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

func (vv VV) SeeNextID(id ID) int {
	return vv.SeeNextSrcSeq(id.Src(), id.SeqOff())
}

// returns VvSeen for already seen, VvGap for causal gaps, VvNext for OK
func (vv VV) SeeNextSrcSeq(src, seq uint32) int {
	val, _ := vv[src]
	if val >= seq {
		return VvSeen
	}
	if val+1 < seq { // FIXME :)
		return VvGap
	}
	vv[src] = seq
	return VvNext
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

func (vv VV) Seen(bb VV) bool {
	for src, bseq := range bb {
		seq := vv[src]
		if bseq > seq {
			return false
		}
	}
	return true
}

func (vv VV) GetLastID(orig uint32) ID {
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
