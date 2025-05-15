package rdx

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/drpcorg/chotki/protocol"
)

type Time struct {
	Rev int64
	Src uint64
}

func (t Time) ZipBytes() []byte {
	return ZipIntUint64Pair(t.Rev, t.Src)
}

func TimeFromZipBytes(zip []byte) (t Time) {
	// todo bad data
	t.Rev, t.Src = UnzipIntUint64Pair(zip)
	return
}

var ErrBadFIRST = errors.New("bad FIRST record")

func MelAppend(to []byte, lit byte, t Time, body []byte) []byte {
	tb := protocol.TinyRecord('T', t.ZipBytes())
	return protocol.Append(to, lit, tb, body)
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
		if time.Src != src {
			ret = make([]byte, at, len(first)*2)
			copy(ret, first[:at])
			break
		}
	}
	if ret == nil && err == nil {
		return first, nil
	}
	for err == nil {
		time.Src = src
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
		return append(tlv, protocol.Record('F', Fparse(string(n.Text)))...)
	case Integer:
		return append(tlv, protocol.Record('I', Iparse(string(n.Text)))...)
	case Reference:
		return append(tlv, protocol.Record('R', Rparse(string(n.Text)))...)
	case String:
		return append(tlv, protocol.Record('S', Sparse(string(n.Text)))...)
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

func Etlv(tlv []byte) []byte {
	return tlv
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
		ih.Push(&MIterator{Val: FIRSTIterator{TLV: tlv}})
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
		if len(it.val) == 0 { // removed
			continue
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

func MnativeSS(tlv []byte) MapSS {
	ret := make(MapSS)
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
		keyrdt, _, key := it.ParsedValue()
		if !it.Next() {
			break
		}
		if len(it.val) == 0 { // removed
			continue
		}
		valrdt, _, val := it.ParsedValue()
		if keyrdt != String || valrdt != String {
			continue
		}
		ret[string(key)] = string(val)
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
type MapSS map[string]string
type MapIB map[int]byte

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

func MtlvTR(nat MapTR) (tlv []byte) {
	pairs := protocol.Records{}
	rev0 := []byte{'0'}
	for k, v := range nat {
		rec := []byte{}
		rec = protocol.Append(rec, 'T', rev0, []byte(k))
		rec = protocol.Append(rec, 'R', rev0, v.ZipBytes())
		pairs = append(pairs, rec)
	}
	valueOrderUnstampedFirsts(pairs)
	return protocol.Join(pairs...)
}

func valueOrderUnstampedFirsts(recs protocol.Records) {
	slices.SortFunc(recs, func(a, b []byte) int {
		return bytes.Compare(a[2:], b[2:])
	})
}

func MtlvTT(mtt MapTT) (tlv []byte) {
	pairs := protocol.Records{}
	rev0 := []byte{'0'}
	for k, v := range mtt {
		rec := []byte{}
		rec = protocol.Append(rec, 'T', rev0, []byte(k))
		rec = protocol.Append(rec, 'T', rev0, []byte(v))
		pairs = append(pairs, rec)
	}
	valueOrderUnstampedFirsts(pairs)
	return protocol.Join(pairs...)
}

func MtlvSS(m MapSS) (tlv []byte) {
	pairs := protocol.Records{}
	rev0 := []byte{'0'}
	for k, v := range m {
		rec := []byte{}
		rec = protocol.Append(rec, 'S', rev0, []byte(k))
		rec = protocol.Append(rec, 'S', rev0, []byte(v))
		pairs = append(pairs, rec)
	}
	valueOrderUnstampedFirsts(pairs)
	return protocol.Join(pairs...)
}

func MtlvII(nat MapTT) (tlv []byte) {
	return nil
}

func NextRev(revz uint64) (next int64) {
	next = ZagZigUint64(revz)
	if next < 0 {
		next = -next
	}
	next++
	return
}

func Mdelta(tlv []byte, new_tlv []byte) (tlv_delta []byte) {
	oldit := MIterator{Val: FIRSTIterator{TLV: tlv}}
	newit := MIterator{Val: FIRSTIterator{TLV: new_tlv}}
	oldok := oldit.Next()
	newok := newit.Next()
	for oldok {
		for newok && FIRSTcompare(oldit.Key.one, newit.Key.one) > 0 {
			tlv_delta = append(tlv_delta, newit.both...)
			newok = newit.Next()
		}
		if newok && FIRSTcompare(oldit.Key.one, newit.Key.one) == 0 {
			if FIRSTcompare(oldit.Val.one, newit.Val.one) != 0 {
				newrev := NextRev(oldit.Val.revz)
				tlv_delta = append(tlv_delta, protocol.Record(oldit.Key.lit, FIRSTtlv(newrev, 0, oldit.Key.val))...)
				tlv_delta = append(tlv_delta, protocol.Record(newit.Val.lit, FIRSTtlv(newrev, 0, newit.Val.val))...)
			}
			oldok = oldit.Next()
			newok = newit.Next()
		} else {
			if len(oldit.Val.val) > 0 {
				newrev := NextRev(oldit.Val.revz)
				tlv_delta = append(tlv_delta, protocol.Record(oldit.Key.lit, FIRSTtlv(newrev, 0, oldit.Key.val))...)
				tlv_delta = append(tlv_delta, protocol.Record(oldit.Val.lit, FIRSTtlv(newrev, 0, []byte{}))...)
			}
			oldok = oldit.Next()
		}
	}
	for newok {
		if len(newit.Val.val) > 0 {
			tlv_delta = append(tlv_delta, newit.both...)
		}
		newok = newit.Next()
	}
	return
}

// Given a TLV of *changed* key-value pairs, produce a delta
func Mdelta2(tlv []byte, changes []byte) (tlv_delta []byte) {
	oldit := MIterator{Val: FIRSTIterator{TLV: tlv}}
	newit := MIterator{Val: FIRSTIterator{TLV: changes}}
	oldok := oldit.Next()
	newok := newit.Next()
	for oldok {
		for newok && FIRSTcompare(oldit.Key.one, newit.Key.one) > 0 {
			tlv_delta = append(tlv_delta, newit.both...)
			newok = newit.Next()
		}
		if newok && FIRSTcompare(oldit.Key.one, newit.Key.one) == 0 {
			if FIRSTcompare(oldit.Val.one, newit.Val.one) != 0 {
				newrev := NextRev(oldit.Val.revz)
				tlv_delta = append(tlv_delta, protocol.Record(oldit.Key.lit, FIRSTtlv(newrev, 0, oldit.Key.val))...)
				tlv_delta = append(tlv_delta, protocol.Record(newit.Val.lit, FIRSTtlv(newrev, 0, newit.Val.val))...)
			}
			oldok = oldit.Next()
			newok = newit.Next()
		} else {
			oldok = oldit.Next()
		}
	}
	for newok {
		if len(newit.Val.val) > 0 {
			tlv_delta = append(tlv_delta, newit.both...)
		}
		newok = newit.Next()
	}
	return
}

func MdeltaTR(tlv []byte, changes MapTR, clock Clock) (tlv_delta []byte) {
	return Mdelta2(tlv, MtlvTR(changes))
}

// checks a TLV value for validity (format violations)
func Mvalid(tlv []byte) bool {
	return true // todo
}

func Mdiff(tlv []byte, vvdiff VV) []byte {
	return nil //todo
}

type MIterator struct {
	Key, Val FIRSTIterator
	both     []byte
}

func (a *MIterator) Next() (got bool) {
	a.both = a.Val.TLV
	if !a.Val.Next() {
		a.both = nil
		return false
	}
	a.Key = a.Val
	if !a.Val.Next() {
		a.both = nil
		return false
	}
	a.both = a.both[:len(a.both)-len(a.Val.TLV)]
	return true
}

func (a *MIterator) Merge(b SortedIterator) int {
	bb := b.(*MIterator)
	cmp := FIRSTcompare(a.Key.one, bb.Key.one)
	if cmp < 0 {
		return MergeAB
	} else if cmp > 0 {
		return MergeBA
	} else if a.Key.revz < bb.Key.revz {
		return MergeB
	} else if a.Key.revz > bb.Key.revz {
		return MergeA
	} else if a.Key.src < bb.Key.src {
		return MergeB
	} else {
		return MergeA
	}

}

func (a *MIterator) Value() []byte {
	return a.both
}

// parse a text form into a TLV value
func Lparse(txt string) (tlv []byte) {
	bm := 0
	bm, tlv = protocol.OpenHeader(tlv, 'B')
	tlv = append(tlv, '0')
	rdx, err := ParseRDX([]byte(txt))
	if err != nil || rdx == nil || rdx.RdxType != Linear {
		return nil
	}
	for i := 0; i < len(rdx.Nested); i++ {
		n := &rdx.Nested[i]
		tlv = appendParsedFirstTlvt(tlv, n, Time{int64(i) + 1, 0})
	}
	protocol.CloseHeader(tlv, bm)
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
	return append(tlv, protocol.Record(rdt, SetTimeFIRST(untimed, t))...)
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

type Rdxable[T comparable] interface {
	comparable
	TLV() []byte
	Type() byte
	Native(tlv []byte) (T, error)
}

type Stamped[T any] struct {
	Time  Time
	Value T
}

func NewStampedMap[K Rdxable[K], T Rdxable[T]]() *StampedMap[K, T] {
	return &StampedMap[K, T]{
		Map: make(map[K]Stamped[T]),
	}
}

type StampedMap[K Rdxable[K], T Rdxable[T]] struct {
	Map map[K]Stamped[T]
}

func (m *StampedMap[K, T]) Set(key K, value T) {
	m.Map[key] = Stamped[T]{Time: Time{Rev: 0, Src: 0}, Value: value}
}

func (m *StampedMap[K, T]) SetStamped(key K, value T, time Time) {
	m.Map[key] = Stamped[T]{Time: Time{Rev: time.Rev, Src: time.Src}, Value: value}
}

func (m *StampedMap[K, T]) Tlv() []byte {
	pairs := protocol.Records{}
	rev0 := []byte{'0'}
	for k, v := range m.Map {
		rec := []byte{}
		rec = protocol.Append(rec, k.Type(), FIRSTtlv(v.Time.Rev, v.Time.Src, k.TLV()))
		rec = protocol.Append(rec, v.Value.Type(), rev0, v.Value.TLV())
		pairs = append(pairs, rec)
	}
	valueOrderUnstampedFirsts(pairs)
	return protocol.Join(pairs...)
}

func (m *StampedMap[K, T]) Native(tlv []byte) error {
	ret := make(map[K]Stamped[T])
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
		keyrdt, _, key := it.ParsedValue()
		if !it.Next() {
			return fmt.Errorf("incomplete record: %d", keyrdt)
		}
		if len(it.val) == 0 { // removed
			continue
		}
		var rdxKey K
		if keyrdt != rdxKey.Type() {
			return fmt.Errorf("invalid key type: %d, expected %d", keyrdt, rdxKey.Type())
		}
		rdxKey, err := rdxKey.Native(key)
		if err != nil {
			return err
		}
		valrdt, time, val := it.ParsedValue()
		var rdxVal T
		if valrdt != rdxVal.Type() {
			return fmt.Errorf("invalid value type: %d, expected %d", valrdt, rdxVal.Type())
		}
		rdxVal, err = rdxVal.Native(val)
		if err != nil {
			return err
		}
		ret[rdxKey] = Stamped[T]{
			Time:  time,
			Value: rdxVal,
		}
	}
	m.Map = ret
	return nil
}

type StampedSet[T Rdxable[T]] struct {
	Value map[T]Time
}

func NewStampedSet[T Rdxable[T]]() *StampedSet[T] {
	return &StampedSet[T]{
		Value: make(map[T]Time),
	}
}

func (l *StampedSet[T]) Add(value T) {
	ll, ok := l.Value[value]
	if !ok {
		l.Value[value] = Time{Rev: 0, Src: 0}
	} else {
		l.Value[value] = Time{Rev: ll.Rev + 1, Src: 0}
	}
}
func (l *StampedSet[T]) AddStamped(value T, rev int64, src uint64) {
	l.Value[value] = Time{Rev: rev, Src: src}
}

func (l *StampedSet[T]) Tlv() []byte {
	pairs := protocol.Records{}
	for v, t := range l.Value {
		rec := []byte{}
		rec = protocol.Append(rec, v.Type(), FIRSTtlv(t.Rev, t.Src, v.TLV()))
		pairs = append(pairs, rec)
	}
	valueOrderUnstampedFirsts(pairs)
	return protocol.Join(pairs...)
}

func (l *StampedSet[T]) Native(tlv []byte) error {
	ret := make(map[T]Time)
	it := FIRSTIterator{TLV: tlv}
	for it.Next() {
		vrdt, time, val := it.ParsedValue()
		var rdxval T
		if vrdt != rdxval.Type() {
			return fmt.Errorf("invalid value type: %d, expected %d", vrdt, rdxval.Type())
		}
		rdxval, err := rdxval.Native(val)
		if err != nil {
			return err
		}
		ret[rdxval] = time
	}
	l.Value = ret
	return nil
}

type RdxString string

func (s RdxString) Type() byte {
	return 'S'
}

func (s RdxString) TLV() []byte {
	return Stlv(string(s))
}

func (s RdxString) Native(tlv []byte) (RdxString, error) {
	return RdxString(Snative(tlv)), nil
}

type RdxInt int64

func (i RdxInt) Type() byte {
	return 'I'
}

func (i RdxInt) TLV() []byte {
	return Itlv(int64(i))
}

func (i RdxInt) Native(tlv []byte) (RdxInt, error) {
	return RdxInt(Inative(tlv)), nil
}

type RdxByte byte

func (b RdxByte) Type() byte {
	return 'S'
}

func (b RdxByte) TLV() []byte {
	return []byte{byte(b)}
}

func (b RdxByte) Native(tlv []byte) (RdxByte, error) {
	return RdxByte(tlv[0]), nil
}

type RdxRid ID

func (r RdxRid) Type() byte {
	return 'R'
}

func (r RdxRid) TLV() []byte {
	return Rtlv(ID(r))
}

func (r RdxRid) Native(tlv []byte) (RdxRid, error) {
	return RdxRid(Rnative(tlv)), nil
}
