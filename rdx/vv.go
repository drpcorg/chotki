package rdx

import (
	"errors"
	"slices"

	"github.com/learn-decentralized-systems/toytlv"
)

// VV is a version vector, max ids seen from each known replica.
type VV map[uint64]uint64

func (vv VV) Get(src uint64) (pro uint64) {
	return vv[src]
}

// Set the progress for the specified source
func (vv VV) Set(src, pro uint64) {
	vv[src] = pro
}

// Put the src-pro pair to the VV, returns whether it was
// unseen (i.e. made any difference)
func (vv VV) Put(src, pro uint64) bool {
	pre, ok := vv[src]
	if ok && pre >= pro {
		return false
	}
	vv[src] = pro
	return true
}

// Adds the id to the VV, returns whether it was unseen
func (vv VV) PutID(id ID) bool {
	return vv.Put(id.Src(), id.Pro())
}

var ErrSeen = errors.New("previously seen id")
var ErrGap = errors.New("id sequence gap")

// Whether this VV overlaps with another one (have common non-zero entry)
func (vv VV) ProgressedOver(b VV) bool {
	for src, pro := range vv {
		bpro, ok := b[src]
		if !ok || pro > bpro {
			return true
		}
	}
	return false
}

func (vv VV) InterestOver(b VV) VV {
	ahead := make(VV)
	for src, pro := range vv {
		bpro, ok := b[src]
		if !ok || pro > bpro {
			ahead[src] = bpro
		}
	}
	return ahead
}

func (vv VV) IDs() (ids []ID) {
	for src, pro := range vv {
		ids = append(ids, IDfromSrcPro(src, pro))
	}
	slices.Sort(ids)
	return
}

// TLV Vv record, nil for empty
func (vv VV) TLV() (ret []byte) {
	ids := vv.IDs()
	for _, id := range ids {
		ret = toytlv.Append(ret, 'V', id.ZipBytes())
	}
	return
}

const ( // todo
	VvSeen = -1
	VvNext = 0
	VvGap  = 1
)

var ErrBadVRecord = errors.New("bad V record")
var ErrBadV0Record = errors.New("bad V0 record")

// consumes: Vv record
func (vv VV) PutTLV(rec []byte) (err error) {
	rest := rec
	for len(rest) > 0 {
		var val []byte
		val, rest, err = toytlv.TakeWary('V', rest)
		if err != nil {
			break
		}
		id := IDFromZipBytes(val)
		vv.PutID(id)
	}
	return
}

func VValid(tlv []byte) bool {
	for len(tlv) > 0 {
		var body []byte
		body, tlv = toytlv.Take('V', tlv)
		if body == nil || len(body) > 16 { //todo valid pair len
			return false
		}
	}
	return true
}

func (vv VV) Seen(bb VV) bool {
	for src, pro := range bb {
		seq := vv[src]
		if pro > seq {
			return false
		}
	}
	return true
}

func (vv VV) GetID(src uint64) ID {
	return IDfromSrcPro(src, vv[src])
}

func (vv VV) String() string {
	ids := vv.IDs()
	ret := make([]byte, 0, len(vv)*32)
	for i := 0; i+1 < len(ids); i++ {
		ret = append(ret, ids[i].String()...)
		ret = append(ret, ',')
	}
	if len(ids) > 0 {
		ret = append(ret, ids[len(ids)-1].String()...)
	}
	return string(ret)
}

func VVFromString(vvs string) (vv VV) {
	vv = make(VV)
	rest := []byte(vvs)
	var id ID
	for len(rest) > 0 && id != BadId {
		id, rest = readIDFromString(rest)
		vv.PutID(id)
	}
	return
}

func VVFromTLV(tlv []byte) (vv VV) {
	vv = make(VV)
	_ = vv.PutTLV(tlv)
	return
}

func Vstring(tlv []byte) (txt string) {
	vv := make(VV)
	_ = vv.PutTLV(tlv)
	return vv.String()
}

func Vparse(txt string) (tlv []byte) {
	vv := VVFromString(txt)
	return Vtlv(vv)
}

func Vtlv(vv VV) (tlv []byte) {
	return vv.TLV()
}

func Vplain(tlv []byte) VV {
	return VVFromTLV(tlv)
}

func Vmerge(tlvs [][]byte) (tlv []byte) {
	vv := make(VV)
	for _, v := range tlvs {
		_ = vv.PutTLV(v)
	}
	tlv = vv.TLV()
	return
}

func Vdelta(tlv []byte, new_val VV) (tlv_delta []byte) {
	old := make(VV)
	delta := make(VV)
	_ = old.PutTLV(tlv)
	for src, pro := range new_val {
		pre := old[src]
		if pro != pre {
			delta.Put(src, pro)
		}
	}
	return delta.TLV()
}

func Vvalid(tlv []byte) bool {
	vv := make(VV)
	return vv.PutTLV(tlv) == nil
}

func Vdiff(tlv []byte, vvdiff VV) (diff_tlv []byte) {
	old := make(VV)
	diff := make(VV)
	_ = old.PutTLV(tlv)
	for src, pro := range old {
		pre, ok := vvdiff[src]
		if ok && pro > pre {
			diff[src] = pro
		}
	}
	return diff.TLV()
}
