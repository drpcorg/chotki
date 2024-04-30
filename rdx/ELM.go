package rdx

import (
	"bytes"
	"errors"
	"github.com/drpcorg/chotki/toytlv"
	"slices"
	"sort"
)

type Time struct {
	rev int64
	src uint64
}

func (t Time) Time64() uint64 {
	return (ZigZagInt64(t.rev) << SrcBits) | t.src
}

const srcMask = (uint64(1) << SrcBits) - 1

func Time64FromRevzSrc(revz, src uint64) uint64 {
	return (revz << SrcBits) | (src & srcMask)
}

func TimeFrom64(t64 uint64) Time {
	return Time{rev: ZagZigUint64(t64 >> SrcBits), src: t64 & srcMask}
}

func (t Time) ZipBytes() []byte {
	return ZipIntUint64Pair(t.rev, t.src)
}

func TimeFromZipBytes(zip []byte) (t Time) {
	// todo bad data
	t.rev, t.src = UnzipIntUint64Pair(zip)
	return
}

var ErrBadFIRST = errors.New("bad FIRST record")

func MelAppend(to []byte, lit byte, t Time, body []byte) []byte {
	tb := toytlv.TinyRecord('T', t.ZipBytes())
	return toytlv.Append(to, lit, tb, body)
}

func MelReSource(first []byte, src uint64) (ret []byte, err error) {
	var lit byte
	var time Time
	var body []byte
	rest := first
	for len(rest) > 0 {
		at := len(first) - len(rest)
		lit, time, body, rest, err = ParseEnvelopedFIRST(rest)
		if err != nil {
			return
		}
		if time.src != src {
			ret = make([]byte, at, len(first)*2)
			copy(ret, first[:at])
			break
		}
	}
	if ret == nil && err == nil {
		return first, nil
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
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
		if ZagZigUint64(it.revz) < 0 {
			continue
		}
		if len(ret) > 1 {
			ret = append(ret, ',')
		}
		ret = appendFirstTlvString(ret, it.lit, it.bare)
	}
	ret = append(ret, '}')
	return string(ret)
}

func appendFirstTlvString(tlv []byte, lit byte, bare []byte) []byte {
	switch lit {
	case 'F':
		return append(tlv, Fstring(bare)...)
	case 'I':
		return append(tlv, Istring(bare)...)
	case 'R':
		return append(tlv, Rstring(bare)...)
	case 'S':
		return append(tlv, Sstring(bare)...)
	case 'T':
		return append(tlv, Tstring(bare)...)
	default:
	}
	return nil
}

// parse a text form into a TLV value
func Eparse(txt string) (tlv []byte) {
	rdx, err := ParseRDX([]byte(txt))
	if err != nil || rdx == nil || rdx.RdxType != Eulerian {
		return nil
	}
	for i := 0; i < len(rdx.Nested); i++ {
		n := &rdx.Nested[i]
		tlv = appendParsedFirstTlv(tlv, n)
	}
	return
}

func appendParsedFirstTlv(tlv []byte, n *RDX) []byte {
	switch n.RdxType {
	case Float:
		return append(tlv, toytlv.Record('F', Fparse(string(n.Text)))...)
	case Integer:
		return append(tlv, toytlv.Record('I', Iparse(string(n.Text)))...)
	case Reference:
		return append(tlv, toytlv.Record('R', Rparse(string(n.Text)))...)
	case String:
		return append(tlv, toytlv.Record('S', Sparse(string(n.Text)))...)
	case Term:
		//ret = append(ret, Tstring(it.val)...) fixme
		return tlv
	default:
		return tlv
	}
}

// merge TLV values
func Emerge(tlvs [][]byte) (merged []byte) {
	ih := ItHeap[*EIterator]{}
	for _, tlv := range tlvs {
		ih.Push(&EIterator{FIRSTIterator{TLV: tlv}})
	}
	for ih.Len() > 0 {
		merged = append(merged, ih.Next()...)
	}
	return
}

// produce an op that turns the old value into the new one
func Edelta(tlv []byte, new_val int64, clock Clock) (tlv_delta []byte) {
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

// M is a map, FIRST keys to FIRST values, {1:2, "three":f-4}

// produce a text form (for REPL mostly)
func Mstring(tlv []byte) (txt string) {
	var ret = []byte{'{'}
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
		if ZagZigUint64(it.revz) < 0 {
			continue
		}
		if len(ret) > 1 {
			ret = append(ret, ',')
		}
		ret = appendFirstTlvString(ret, it.lit, it.bare)
		ret = append(ret, ':')
		if !it.Next() {
			ret = append(ret, "null"...)
			break
		}
		if ZagZigUint64(it.revz) < 0 {
			ret = append(ret, "null"...)
		} else {
			ret = appendFirstTlvString(ret, it.lit, it.bare)
		}
	}
	ret = append(ret, '}')
	return string(ret)
}

type kv struct {
	k, v []byte
}

// parse a text form into a TLV value
func Mparse(txt string) (tlv []byte) {
	rdx, err := ParseRDX([]byte(txt))
	if err != nil || rdx == nil || rdx.RdxType != Mapping {
		return nil
	}
	kvs := []kv{}
	pairs := rdx.Nested
	for i := 0; i+1 < len(pairs); i += 2 {
		next := kv{
			FIRSTparsee(pairs[i].RdxType, string(pairs[i].Text)),
			FIRSTparsee(pairs[i+1].RdxType, string(pairs[i+1].Text)),
		}
		if next.k != nil && next.v != nil {
			kvs = append(kvs, next)
		}
	}
	slices.SortFunc(kvs, func(i, j kv) int {
		return FIRSTcompare(i.k, j.k)
	})
	for _, x := range kvs {
		tlv = append(tlv, x.k...)
		tlv = append(tlv, x.v...)
	}
	return
}

// merge TLV values
func Mmerge(tlvs [][]byte) (merged []byte) {
	ih := ItHeap[*MIterator]{}
	for _, tlv := range tlvs {
		ih.Push(&MIterator{it: FIRSTIterator{TLV: tlv}})
	}
	for ih.Len() > 0 {
		merged = append(merged, ih.Next()...)
	}
	return
}

func MnativeTT(tlv []byte) MapTT {
	ret := make(MapTT)
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
		keyrdt, _, key := it.ParsedValue()
		if !it.Next() {
			break
		}
		valrdt, _, val := it.ParsedValue()
		if keyrdt != Term || valrdt != Term {
			continue
		}
		ret[string(key)] = string(val)
	}
	return ret
}

func MnativeTR(tlv []byte) MapTR {
	ret := make(MapTR)
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
		keyrdt, _, key := it.ParsedValue()
		if !it.Next() {
			break
		}
		valrdt, _, val := it.ParsedValue()
		if keyrdt != Term || valrdt != Reference {
			continue
		}
		id := IDFromZipBytes(val)
		ret[string(key)] = id
	}
	return ret
}

func MparseTR(arg *RDX) MapTR {
	if arg == nil || arg.RdxType != Mapping {
		return nil
	}
	ret := make(MapTR)
	kvs := arg.Nested
	for i := 0; i+1 < len(kvs); i += 2 {
		if kvs[i].RdxType != Term || kvs[i+1].RdxType != Reference {
			continue
		}
		ret[string(kvs[i].Text)] = IDFromText(kvs[i+1].Text)
	}
	return ret
}

type MapTR map[string]ID
type MapTT map[string]string

func (m MapTR) String() string {
	var keys []string
	for key := range m {
		keys = append(keys, key)
	}
	var ret []byte
	ret = append(ret, '{', '\n')
	for _, key := range keys {
		ret = append(ret, '\t')
		ret = append(ret, key...)
		ret = append(ret, ':', '\t')
		ret = append(ret, m[key].String()...)
		ret = append(ret, ',', '\n')
	}
	ret = append(ret, '}')
	return string(ret)
}

// produce an op that turns the old value into the new one
func MdeltaTR(tlv []byte, changes MapTR, clock Clock) (tlv_delta []byte) {
	it := MIterator{it: FIRSTIterator{TLV: tlv}}
	for it.Next() {
		if it.lit != Term {
			continue
		}
		change, ok := changes[string(it.val)]
		if !ok {
			continue
		}
		new_rev := ZagZigUint64(it.revz)
		if new_rev < 0 {
			new_rev = -new_rev
		}
		new_rev++
		tlv_delta = append(tlv_delta, toytlv.Record(Term, FIRSTtlv(new_rev, 0, it.val))...)
		tlv_delta = append(tlv_delta, toytlv.Record(Reference, FIRSTtlv(new_rev, 0, change.ZipBytes()))...)
		delete(changes, string(it.val))
	}
	for key, val := range changes {
		tlv_delta = append(tlv_delta, toytlv.Record(Term, Ttlv(key))...)
		tlv_delta = append(tlv_delta, toytlv.Record(Reference, Rtlv(val))...)
	}
	return
}

// checks a TLV value for validity (format violations)
func Mvalid(tlv []byte) bool {
	return true // todo
}

func Mdiff(tlv []byte, vvdiff VV) []byte {
	return nil //todo
}

type MIterator struct {
	it   FIRSTIterator
	keye []byte
	val  []byte
	src  uint64
	revz uint64
	lit  byte
	pair []byte
}

func (a *MIterator) Next() (got bool) {
	tlv := a.it.TLV
	got = a.it.Next() // skip value
	a.keye = a.it.one
	a.val = a.it.val
	a.src = a.it.src
	a.revz = a.it.revz
	a.lit = a.it.lit
	if got {
		got = a.it.Next()
	}
	a.pair = tlv[:len(tlv)-len(a.it.TLV)]
	return
}

func (a *MIterator) Merge(b SortedIterator) int {
	bb := b.(*MIterator)
	cmp := FIRSTcompare(a.keye, bb.keye)
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

func (a *MIterator) Value() []byte {
	return a.pair
}

// L is an array of any FIRST elements

// produce a text form (for REPL mostly)
func Lstring(tlv []byte) (txt string) {
	var ret = make([]byte, 0, len(tlv)*4)
	rest := tlv
	for len(rest) > 0 {
		var sub []byte
		sub, rest = toytlv.Take('B', rest)
		_, subb := toytlv.Take('T', sub)
		it := LIterator{FIRSTIterator{TLV: subb}}
		for it.Next() {
			if ZagZigUint64(it.revz) < 0 {
				continue
			}
			ret = append(ret, ',')
			ret = appendFirstTlvString(ret, it.lit, it.bare)
		}

	}
	if len(ret) == 0 {
		return "[]"
	}
	ret[0] = '['
	ret = append(ret, ']')
	return string(ret)
}

// parse a text form into a TLV value
func Lparse(txt string) (tlv []byte) {
	bm := 0
	bm, tlv = toytlv.OpenHeader(tlv, 'B')
	tlv = append(tlv, '0')
	rdx, err := ParseRDX([]byte(txt))
	if err != nil || rdx == nil || rdx.RdxType != Linear {
		return nil
	}
	for i := 0; i < len(rdx.Nested); i++ {
		n := &rdx.Nested[i]
		tlv = appendParsedFirstTlvt(tlv, n, Time{int64(i) + 1, 0})
	}
	toytlv.CloseHeader(tlv, bm)
	return
}

func appendParsedFirstTlvt(tlv []byte, n *RDX, t Time) []byte {
	var untimed []byte
	var rdt byte
	switch n.RdxType {
	case Float:
		rdt = 'F'
		untimed = Fparse(string(n.Text))
	case Integer:
		rdt = 'I'
		untimed = Iparse(string(n.Text))
	case Reference:
		rdt = 'R'
		untimed = Rparse(string(n.Text))
	case String:
		rdt = 'S'
		untimed = Sparse(string(n.Text))
	case Term:
		//ret = append(ret, Tstring(it.val)...) fixme
		return nil // todo
	default:
		return nil
	}
	return append(tlv, toytlv.Record(rdt, SetTimeFIRST(untimed, t))...)
}

// merge TLV values
func Lmerge(tlvs [][]byte) (merged []byte) {
	ins := make(map[uint64][][]byte)
	var locs []uint64
	for _, input := range tlvs {
		rest := input
		for len(rest) > 0 {
			var sub []byte
			sub, rest = toytlv.Take('B', rest)
			ref, bare := toytlv.Take('T', sub)
			t := TimeFromZipBytes(ref)
			loc := t.Time64()
			pre, ok := ins[loc]
			if !ok {
				locs = append(locs, loc)
			}
			ins[loc] = append(pre, bare)
		}
	}
	sort.Slice(locs, func(i, j int) bool {
		return locs[i] < locs[j]
	})
	ih := ItHeap[*LIterator]{}
	for len(locs) > 0 {
		into := locs[0]
		locs = locs[1:]
		bares, ok := ins[into]
		if !ok {
			continue
		}
		delete(ins, into)
		if len(ins) == 0 {
			ins = nil
		}
		bm := 0
		bm, merged = toytlv.OpenHeader(merged, 'B')
		merged = toytlv.AppendTiny(merged, 'T', ID(into).ZipBytes())
		pileUp(&ih, bares)
		for ih.Len() > 0 {
			key := Time64FromRevzSrc(ih[0].revz, ih[0].src)
			next := ih.Next()
			merged = append(merged, next...)
			if ins != nil {
				descs, found := ins[key]
				if found {
					pileUp(&ih, descs)
					delete(ins, key)
					if len(ins) == 0 {
						ins = nil
					}
				}
			}
		}
		toytlv.CloseHeader(merged, bm)
	}

	return
}

func pileUp(heap *ItHeap[*LIterator], bares [][]byte) {
	for _, bare := range bares {
		heap.Push(&LIterator{FIRSTIterator{TLV: bare}})
	}
}

// produce an op that turns the old value into the new one
func Ldelta(tlv []byte, new_val int64, clock Clock) (tlv_delta []byte) {
	return nil // todo
}

// checks a TLV value for validity (format violations)
func Lvalid(tlv []byte) bool {
	return true // todo
}

func Ldiff(tlv []byte, vvdiff VV) []byte {
	return nil //todo
}

type LIterator struct {
	FIRSTIterator
}

func (a *LIterator) Merge(bb SortedIterator) int {
	b := bb.(*LIterator)
	if a.revz > b.revz {
		return MergeAB
	} else if a.revz < b.revz {
		return MergeBA
	} else if a.src > b.src {
		return MergeAB
	} else if a.src < b.src {
		return MergeBA
	} else {
		return MergeA
	}
}

func Mrdx2tlv(a *RDX) (tlv []byte) {
	if a == nil || a.RdxType != Mapping {
		return nil
	}
	for i := 0; i+1 < len(a.Nested); i += 2 {
		tlv = append(tlv, FIRSTrdx2tlv(&a.Nested[i])...)
		tlv = append(tlv, FIRSTrdx2tlv(&a.Nested[i+1])...)
	}
	return
}

func ELMdefault() (tlv []byte) {
	return []byte{}
}

func ELMstring(tlv []byte) string {
	ret := []byte{}
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
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
			ret = append(ret, Tstring(it.bare)...)
		default:
			ret = append(ret, '?')
		}
		ret = append(ret, ',')
	}
	return string(ret)
}
