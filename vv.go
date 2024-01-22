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

func (vv *VV) Put(orig, val uint32) {
	(*vv)[orig] = val
}

var ErrSeen = errors.New("previously seen id")
var ErrGap = errors.New("id sequence gap")

func (vv *VV) PutSeq(orig, val uint32) error {
	has, _ := (*vv)[orig]
	if has >= val {
		return ErrSeen
	} else if has+1 < val {
		return ErrGap
	} else {
		(*vv)[orig] = val
		return nil
	}
}

// TLV Vv record
func (vv *VV) Bytes() (ret []byte) {
	bm, ret := toytlv.OpenHeader(ret, 'V')
	for orig, seq := range *vv {
		ret = toytlv.Append(ret, 'V', ZipUint64Pair(uint64(seq), uint64(orig)))
	}
	toytlv.CloseHeader(ret, bm)
	return
}

// Ignores already-seen; returns ErrGap on sequence gaps
func (vv *VV) Filter(batch Batch) (new_batch Batch, err error) {
	new_batch = make(Batch, 0, len(batch))
	for _, pack := range batch {
		seq, src := PacketID(pack)
		val, _ := (*vv)[src]
		if val >= seq { // fixme double parsing
			continue
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
func (vv *VV) LoadBytes(rec []byte) error {
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
		have := (*vv)[src]
		if seq > have {
			(*vv)[src] = seq
		}
	}
	return nil
}
