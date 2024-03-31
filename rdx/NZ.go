package rdx

import (
	"fmt"
	"github.com/learn-decentralized-systems/toytlv"
)

// N is an increment-only uint64 counter

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

func (a *NIterator) Next() bool {
	if len(a.tlv) == 0 {
		return false
	}
	_, hlen, blen := toytlv.ProbeHeader(a.tlv)
	rlen := hlen + blen
	a.inc, a.src = UnzipUint64Pair(a.tlv[hlen:rlen])
	a.one = a.tlv[:rlen]
	a.tlv = a.tlv[rlen:]
	return true
}

func (a *NIterator) Merge(b SortedIterator) int {
	bb := b.(*NIterator)
	if a.src == bb.src {
		if a.inc < bb.inc {
			return MergeB
		} else {
			return MergeA
		}
	} else if a.src < bb.src {
		return MergeAB
	} else {
		return MergeBA
	}
}

func (a *NIterator) Value() []byte {
	return a.one
}

// Z is a two-way int64 counter

// produce a text form (for REPL mostly)
func Zstring(tlv []byte) (txt string) {
	return fmt.Sprintf("%d", Znative(tlv))
}

// parse a text form into a TLV value
func Zparse(txt string) (tlv []byte) {
	var i int64
	_, _ = fmt.Sscanf(txt, "%d", &i)
	return Ztlv(i)
}

// convert native golang value into a TLV form
func Ztlv(i int64) (tlv []byte) {
	return toytlv.Record('I',
		toytlv.TinyRecord('T', ZipIntUint64Pair(0, 0)),
		ZipInt64(i),
	)
}

// convert a TLV value to a native golang value
func Znative(tlv []byte) (sum int64) {
	rest := tlv
	for len(rest) > 0 {
		var one []byte
		one, rest = toytlv.Take('I', rest)
		_, body := toytlv.Take('T', one)
		inc := UnzipInt64(body)
		sum += inc
	}
	return
}

// merge TLV values
func Zmerge(tlvs [][]byte) (merged []byte) {
	ih := ItHeap[*ZIterator]{}
	for _, tlv := range tlvs {
		ih.Push(&ZIterator{FIRSTIterator{tlv: tlv}})
	}
	for ih.Len() > 0 {
		merged = append(merged, ih.Next()...)
	}
	return
}

// produce an op that turns the old value into the new one
func Zdelta(tlv []byte, new_val int64) (tlv_delta []byte) {
	sum := Znative(tlv)
	if new_val == sum {
		return []byte{}
	}
	return Ztlv(new_val - sum)
}

// checks a TLV value for validity (format violations)
func Zvalid(tlv []byte) bool {
	return true //todo
}

func Zdiff(tlv []byte, vvdiff VV) []byte {
	return nil //fixme
}

type ZIterator struct {
	FIRSTIterator
}

func (a *ZIterator) Merge(b SortedIterator) int {
	bb := b.(*ZIterator)
	if a.src == bb.src {
		if a.revz < bb.revz {
			return MergeB
		} else {
			return MergeA
		}
	} else if a.src < bb.src {
		return MergeAB
	} else {
		return MergeBA
	}
}
