package main

import (
	"errors"
	"github.com/learn-decentralized-systems/toytlv"
)

type VV map[uint32]uint32

func (vv VV) Get(orig uint32) (val uint32) {
	val, _ = vv[orig]
	return
}

func (vv VV) Set(src, seq uint32) {
	vv[src] = seq
}

// Adds the src-seq pair to the VV, returns whether it was unseen
func (vv VV) Put(src, seq uint32) bool {
	pre, ok := vv[src]
	if ok && pre >= seq {
		return false
	}
	vv[src] = seq
	return true
}

// Adds the id to the VV, returns whether it was unseen
func (vv VV) PutID(id ID) bool {
	return vv.Put(id.Src(), id.Seq())
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
func (vv VV) TLV() (ret []byte) {
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
func (vv VV) AddTLV(rec []byte) error {
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

func (vv VV) GetID(orig uint32) ID {
	seq := vv.Get(orig)
	return MakeID(orig, seq, 0)
}
