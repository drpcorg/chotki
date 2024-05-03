package rdx

import (
	"fmt"
	"github.com/drpcorg/chotki/protocol"
	"strconv"
)

// N is an increment-only uint64 counter

// produce a text form (for REPL mostly)
func Nstring(tlv []byte) (txt string) {
	return fmt.Sprintf("%d", Nnative(tlv))
}

func Ndefault() []byte { return []byte{} }

// parse a text form into a TLV value
func Nparse(txt string) (tlv []byte) {
	u, err := strconv.ParseUint(txt, 10, 64)
	if err != nil {
		return nil
	}
	return Ntlv(u)
}

// convert a native golang value into TLV
func Ntlv(u uint64) (tlv []byte) {
	time := ZipUint64Pair(u, 0)
	return protocol.Record(Term, protocol.TinyRecord('T', time))
}

func Ntlvt(inc uint64, src uint64) []byte {
	return protocol.Record(Term, protocol.TinyRecord('T', ZipUint64Pair(inc, src)))
}

// convert a TLV value to a native golang value
func Nnative(tlv []byte) (sum uint64) {
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
		sum += it.revz
	}
	return
}

// merge TLV values
func Nmerge(tlvs [][]byte) (merged []byte) {
	ih := ItHeap[*NIterator]{}
	for _, tlv := range tlvs {
		ih.Push(&NIterator{FIRSTIterator{TLV: tlv}})
	}
	for ih.Len() > 0 {
		merged = append(merged, ih.Next()...)
	}
	return
}

func N2string(tlv []byte, new_val string, src uint64) (tlv_delta []byte) {
	if len(new_val) == 0 {
		return nil
	}
	it := NIterator{FIRSTIterator{TLV: tlv}}
	mine := uint64(0)
	for it.Next() {
		if it.src == src {
			mine = it.revz
			break
		}
	}
	add := false
	if new_val[0] == '+' {
		add = true
		new_val = new_val[1:]
	}
	var num uint64
	n, _ := fmt.Sscanf(new_val, "%d", &num)
	if n < 1 {
		return nil
	}
	if add {
		mine += num
	} else if num < mine {
		return nil
	} else if num == mine {
		return nil
	} else {
		mine = num
	}
	return ZipUint64Pair(mine, 0)
}

// produce an op that turns the old value into the new one
// return nil on error, empty slice for "no changes"
func Ndelta(tlv []byte, new_val uint64, clock Clock) (tlv_delta []byte) {
	it := FIRSTIterator{TLV: tlv}
	max_revz := uint64(0)
	mysrc := clock.Src()
	old_val := uint64(0)
	sum := uint64(0)
	for it.Next() {
		if it.revz > max_revz {
			max_revz = it.revz
		}
		val := UnzipUint64(it.val)
		if it.src == mysrc {
			old_val = val
		}
		sum += val
	}
	if new_val < sum {
		return nil
	} else if new_val == sum {
		return []byte{}
	}
	return Ntlv(new_val - sum + old_val)
}

// checks a TLV value for validity (format violations)
func Nvalid(tlv []byte) bool {
	return true //todo
}

func Ndiff(tlv []byte, vvdiff VV) []byte {
	return tlv //fixme
}

func Nmine(tlv []byte, src uint64) uint64 {
	it := NIterator{FIRSTIterator{TLV: tlv}}
	for it.Next() {
		if it.src == src {
			return it.revz
		}
	}
	return 0
}

type NIterator struct {
	FIRSTIterator
}

func (a *NIterator) Merge(b SortedIterator) int {
	bb := b.(*NIterator)
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
	i, err := strconv.ParseInt(txt, 10, 64)
	if err != nil {
		return nil
	}
	return Ztlv(i)
}

// convert a native golang value into TLV
func Ztlv(i int64) (tlv []byte) {
	return protocol.Record('I',
		protocol.TinyRecord('T', ZipIntUint64Pair(0, 0)),
		ZipInt64(i),
	)
}

// convert a TLV value to a native golang value
func Znative(tlv []byte) (sum int64) {
	rest := tlv
	for len(rest) > 0 {
		var one []byte
		one, rest = protocol.Take('I', rest)
		_, body := protocol.Take('T', one)
		inc := UnzipInt64(body)
		sum += inc
	}
	return
}

// merge TLV values
func Zmerge(tlvs [][]byte) (merged []byte) {
	ih := ItHeap[*ZIterator]{}
	for _, tlv := range tlvs {
		ih.Push(&ZIterator{FIRSTIterator{TLV: tlv}})
	}
	for ih.Len() > 0 {
		merged = append(merged, ih.Next()...)
	}
	return
}

// produce an op that turns the old value into the new one
func Zdelta(tlv []byte, new_val int64, clock Clock) (tlv_delta []byte) {
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
