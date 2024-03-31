package rdx

import (
	"bytes"
	"errors"
	"github.com/learn-decentralized-systems/toytlv"
)

type Time struct {
	rev int64
	src uint64
}

func (t Time) ZipBytes() []byte {
	return ZipIntUint64Pair(t.rev, t.src)
}

func TimeFromZipBytes(zip []byte) (t Time) {
	// todo bad data
	t.rev, t.src = UnzipIntUint64Pair(zip)
	return
}

var ErrBadISFR = errors.New("bad ISFR record")

func MelAppend(to []byte, lit byte, t Time, body []byte) []byte {
	tb := toytlv.TinyRecord('T', t.ZipBytes())
	return toytlv.Append(to, lit, tb, body)
}

func MelReSource(isfr []byte, src uint64) (ret []byte, err error) {
	var lit byte
	var time Time
	var body []byte
	rest := isfr
	for len(rest) > 0 {
		at := len(isfr) - len(rest)
		lit, time, body, rest, err = ParseEnvelopedFIRST(rest)
		if err != nil {
			return
		}
		if time.src != src {
			ret = make([]byte, at, len(isfr)*2)
			copy(ret, isfr[:at])
			break
		}
	}
	if ret == nil && err == nil {
		return isfr, nil
	}
	for err == nil {
		time.src = src
		ret = MelAppend(ret, lit, time, body)
		if len(rest) == 0 {
			break
		}
		lit, time, body, rest, err = ParseEnvelopedFIRST(rest)
	}
	return
}

// E is a set of any FIRST elements

// produce a text form (for REPL mostly)
func Estring(tlv []byte) (txt string) {
	var ret = []byte{'{'}
	it := FIRSTIterator{tlv: tlv}
	for it.Next() {
		if ZagZigUint64(it.revz) < 0 {
			continue
		}
		if len(ret) > 1 {
			ret = append(ret, ',')
		}
		switch it.lit {
		case 'F':
			ret = append(ret, Fstring(it.bare)...)
		case 'I':
			ret = append(ret, Istring(it.bare)...)
		case 'R':
			ret = append(ret, Rstring(it.bare)...)
		case 'S':
			ret = append(ret, Sstring(it.bare)...)
		case 'T':
			//ret = append(ret, Tstring(it.bare)...)
		default:
		}
	}
	ret = append(ret, '}')
	return string(ret)
}

// parse a text form into a TLV value
func Eparse(txt string) (tlv []byte) {
	rdx, err := ParseRDX([]byte(txt))
	if err != nil || rdx == nil || rdx.RdxType != RdxSet {
		return nil
	}
	for i := 0; i < len(rdx.Nested); i++ {
		n := &rdx.Nested[i]
		switch n.RdxType {
		case RdxFloat:
			tlv = append(tlv, toytlv.Record('F', Fparse(string(n.Text)))...)
		case RdxInt:
			tlv = append(tlv, toytlv.Record('I', Iparse(string(n.Text)))...)
		case RdxRef:
			tlv = append(tlv, toytlv.Record('R', Rparse(string(n.Text)))...)
		case RdxString:
			tlv = append(tlv, toytlv.Record('S', Sparse(string(n.Text)))...)
		case RdxTomb:
			//ret = append(ret, Tstring(it.val)...)
		default:
		}
	}
	return
}

// merge TLV values
func Emerge(tlvs [][]byte) (merged []byte) {
	ih := ItHeap[*EIterator]{}
	for _, tlv := range tlvs {
		ih.Push(&EIterator{FIRSTIterator{tlv: tlv}})
	}
	for ih.Len() > 0 {
		merged = append(merged, ih.Next()...)
	}
	return
}

// produce an op that turns the old value into the new one
func Edelta(tlv []byte, new_val int64) (tlv_delta []byte) {
	return nil // todo
}

// checks a TLV value for validity (format violations)
func Evalid(tlv []byte) bool {
	return true // todo
}

func Ediff(tlv []byte, vvdiff VV) []byte {
	return nil //todo
}

type EIterator struct {
	FIRSTIterator
}

func (a *EIterator) Merge(b SortedIterator) int {
	bb := b.(*EIterator)
	cmp := bytes.Compare(a.val, bb.val)
	if cmp < 0 {
		return MergeAB
	} else if cmp > 0 {
		return MergeBA
	} else if a.revz < bb.revz {
		return MergeB
	} else if a.revz > bb.revz {
		return MergeA
	} else if a.src < bb.src {
		return MergeB
	} else {
		return MergeA
	}

}
