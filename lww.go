package main

import (
	"bytes"
	"fmt"
	"github.com/learn-decentralized-systems/toytlv"
)

// shared functions

// for bad format, value==nil (an empty value is an empty slice)
func LWWparse(bulk []byte) (time, src uint64, value []byte) {
	lit, hlen, blen := toytlv.ProbeHeader(bulk)
	if lit != 'T' && lit != '0' || hlen+blen > len(bulk) {
		return
	}
	tsb := bulk[hlen : hlen+blen]
	time, src = UnzipUint64Pair(tsb)
	value = bulk[hlen+blen:]
	return
}

func LWWtlv(time, src uint64, value []byte) (bulk []byte) {
	pair := ZipUint64Pair(time, src)
	bulk = make([]byte, 1, len(pair)+len(value))
	bulk[0] = '0' + byte(len(pair))
	return append(append(bulk, pair...), value...)
}

func LWWmerge(tlvs [][]byte) (tlv []byte) {
	var win []byte
	var maxt uint64
	for _, rec := range tlvs {
		l, hlen, blen := toytlv.ProbeHeader(rec)
		tsb := rec[hlen : hlen+blen]
		time, _ := UnzipUint64Pair(tsb)
		if l != 'T' && l != '0' {
			continue
		}
		if time > maxt || (time == maxt && bytes.Compare(rec, win) > 0) {
			maxt = time
			win = rec
		}
	}
	return win
}

func LWWdiff(tlv []byte, vvdiff VV) []byte {
	_, src, _ := LWWparse(tlv)
	_, ok := vvdiff[src]
	if ok {
		return tlv
	} else {
		return nil
	}
}

// I is a last-write-wins int64

// produce a text form (for REPL mostly)
func Istring(tlv []byte) (txt string) {
	return fmt.Sprintf("%d", Iplain(tlv))
}

// parse a text form into a TLV value
func Iparse(txt string) (tlv []byte) {
	var i int64
	_, _ = fmt.Sscanf(txt, "%d", &i)
	return Itlv(i)
}

var time0 = []byte{0, 0, 0, 0}

// convert plain golang value into a TLV form
func Itlv(i int64) (tlv []byte) {
	return LWWtlv(0, 0, ZipInt64(i))
}

// convert a TLV value to a plain golang value
func Iplain(tlv []byte) int64 {
	_, _, val := LWWparse(tlv)
	return UnzipInt64(val)
}

// merge TLV values
func Imerge(tlvs [][]byte) (tlv []byte) {
	return LWWmerge(tlvs)
}

// produce an op that turns the old value into the new one
func Idelta(tlv []byte, new_val int64) (tlv_delta []byte) {
	time, _, val := LWWparse(tlv)
	nv := ZipInt64(new_val)
	if bytes.Compare(val, nv) != 0 {
		tlv_delta = LWWtlv(time+1, 0, nv)
	}
	return
}

// checks a TLV value for validity (format violations)
func Ivalid(tlv []byte) bool {
	_, src, val := LWWparse(tlv)
	return val != nil && len(val) <= 8 && src < (1<<SrcBits)
}

func Idiff(tlv []byte, vvdiff VV) []byte {
	return LWWdiff(tlv, vvdiff)
}

// S is a last-write-wins UTF-8 string

const hex = "0123456789abcdef"

// produce a text form (for REPL mostly)
func Sstring(tlv []byte) (txt string) {
	dst := make([]byte, 0, len(tlv)*2)
	_, _, val := LWWparse(tlv)
	dst = append(dst, '"')
	for _, b := range val {
		switch b {
		case '\\', '"':
			dst = append(dst, '\\', b)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0xb, 0xc, 0xe, 0xf,
			0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a,
			0x1b, 0x1c, 0x1d, 0x1e, 0x1f:
			dst = append(dst, '\\', 'u', '0', '0', hex[b>>4], hex[b&0xF])
		default:
			dst = append(dst, b)
		}
	}
	dst = append(dst, '"')
	return string(dst)
}

// parse a text form into a TLV value; nil on error
func Sparse(txt string) (tlv []byte) {
	unq := txt[1 : len(txt)-1]
	unesc, _ := Unescape([]byte(unq), nil)
	return LWWtlv(0, 0, unesc)
}

// convert plain golang value into a TLV form
func Stlv(s string) (tlv []byte) {
	return LWWtlv(0, 0, []byte(s))
}

// convert a TLV value to a plain golang value
func Splain(tlv []byte) string {
	_, _, val := LWWparse(tlv)
	return string(val)
}

// merge TLV values
func Smerge(tlvs [][]byte) (tlv []byte) {
	return LWWmerge(tlvs)
}

// produce an op that turns the old value into the new one
func Sdelta(tlv []byte, new_val string) (tlv_delta []byte) {
	time, _, val := LWWparse(tlv)
	nv := []byte(new_val)
	if bytes.Compare(val, nv) != 0 {
		tlv_delta = LWWtlv(time+1, 0, nv)
	}
	return
}

// checks a TLV value for validity (format violations)
func Svalid(tlv []byte) bool {
	_, src, val := LWWparse(tlv)
	return val != nil && src < (1<<SrcBits)
}

func Sdiff(tlv []byte, vvdiff VV) []byte {
	return LWWdiff(tlv, vvdiff)
}

// R is a last-write-wins id64

// produce a text form (for REPL mostly)
func Rstring(tlv []byte) (txt string) {
	return Rplain(tlv).String()
}

// parse a text form into a TLV value
func Rparse(txt string) (tlv []byte) {
	id := IDFromString([]byte(txt))
	return Rtlv(id)
}

// convert plain golang value into a TLV form
func Rtlv(i id64) (tlv []byte) {
	return LWWtlv(0, 0, i.ZipBytes())
}

// convert a TLV value to a plain golang value
func Rplain(tlv []byte) id64 {
	_, _, val := LWWparse(tlv)
	return IDFromZipBytes(val)
}

// merge TLV values
func Rmerge(tlvs [][]byte) (tlv []byte) {
	return LWWmerge(tlvs)
}

// produce an op that turns the old value into the new one
func Rdelta(tlv []byte, new_val id64) (tlv_delta []byte) {
	time, _, val := LWWparse(tlv)
	nv := new_val.ZipBytes()
	if bytes.Compare(val, nv) != 0 {
		tlv_delta = LWWtlv(time+1, 0, nv)
	}
	return
}

// checks a TLV value for validity (format violations)
func Rvalid(tlv []byte) bool {
	_, src, val := LWWparse(tlv)
	return val != nil && src < (1<<SrcBits)
	// todo correct sizes
}

func Rdiff(tlv []byte, vvdiff VV) []byte {
	return LWWdiff(tlv, vvdiff)
}

// F is a last-write-wins float64

// produce a text form (for REPL mostly)
func Fstring(tlv []byte) (txt string) {
	return fmt.Sprintf("%f", Fplain(tlv))
}

// parse a text form into a TLV value
func Fparse(txt string) (tlv []byte) {
	var i float64
	_, _ = fmt.Sscanf(txt, "%f", &i)
	return Ftlv(i)
}

// convert plain golang value into a TLV form
func Ftlv(i float64) (tlv []byte) {
	return LWWtlv(0, 0, ZipFloat64(i))
}

// convert a TLV value to a plain golang value
func Fplain(tlv []byte) float64 {
	_, _, val := LWWparse(tlv)
	return UnzipFloat64(val)
}

// merge TLV values
func Fmerge(tlvs [][]byte) (tlv []byte) {
	return LWWmerge(tlvs)
}

// produce an op that turns the old value into the new one
func Fdelta(tlv []byte, new_val float64) (tlv_delta []byte) {
	time, _, val := LWWparse(tlv)
	nv := ZipFloat64(new_val)
	if bytes.Compare(val, nv) != 0 {
		tlv_delta = LWWtlv(time+1, 0, nv)
	}
	return
}

// checks a TLV value for validity (format violations)
func Fvalid(tlv []byte) bool {
	_, src, val := LWWparse(tlv)
	return val != nil && src < (1<<SrcBits) && len(val) <= 8
}

func Fdiff(tlv []byte, vvdiff VV) []byte {
	return LWWdiff(tlv, vvdiff)
}
