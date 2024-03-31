package rdx

import (
	"fmt"
	"github.com/learn-decentralized-systems/toytlv"
)

// I is a last-write-wins int64

// produce a text form (for REPL mostly)
func Nstring(tlv []byte) (txt string) {
	return fmt.Sprintf("%d", Nnative(tlv))
}

// parse a text form into a TLV value
func Nparse(txt string) (tlv []byte) {
	var u uint64
	_, _ = fmt.Sscanf(txt, "%d", &u)
	return Ntlv(u)
}

// convert native golang value into a TLV form
func Ntlv(u uint64) (tlv []byte) {
	return toytlv.Record('U', ZipUint64Pair(u, 0))
}

// convert a TLV value to a native golang value
func Nnative(tlv []byte) (sum uint64) {
	rest := tlv
	for len(rest) > 0 {
		var one []byte
		one, rest = toytlv.Take('U', rest)
		inc, _ := UnzipUint64Pair(one)
		sum += inc
	}
	return
}

// merge TLV values
func Nmerge(tlvs [][]byte) (merged []byte) {
	ih := ItHeap[*NIterator]{}
	for _, tlv := range tlvs {
		ih.Push(&NIterator{tlv: tlv})
	}
	for ih.Len() > 0 {
		merged = append(merged, ih.Next()...)
	}
	return
}

// produce an op that turns the old value into the new one
func Ndelta(tlv []byte, new_val uint64) (tlv_delta []byte) {
	sum := Nnative(tlv)
	if new_val < sum {
		return nil
	} else if new_val == sum {
		return []byte{}
	}
	return Ntlv(new_val - sum)
}

// checks a TLV value for validity (format violations)
func Nvalid(tlv []byte) bool {
	return true //todo
}

func Ndiff(tlv []byte, vvdiff VV) []byte {
	return nil //fixme
}

type NIterator struct {
	one []byte
	tlv []byte
	src uint64
	inc uint64
}

func (ni *NIterator) Next() bool {
	if len(ni.tlv) == 0 {
		return false
	}
	_, hlen, blen := toytlv.ProbeHeader(ni.tlv)
	rlen := hlen + blen
	ni.inc, ni.src = UnzipUint64Pair(ni.tlv[hlen:rlen])
	ni.one = ni.tlv[:rlen]
	ni.tlv = ni.tlv[rlen:]
	return true
}

func (ni *NIterator) Merge(b SortedIterator) int {
	bb := b.(*NIterator)
	ac := ni.src
	bc := bb.src
	if ac == bc {
		return MergeA
	} else if ac < bc {
		return MergeAB
	} else {
		return MergeBA
	}
}

func (ni *NIterator) Value() []byte {
	return ni.one
}
