package main

import (
	"encoding/binary"
	"fmt"
	"github.com/learn-decentralized-systems/toytlv"
	"math"
)

// shared functions

func LWWvalue(tlv []byte) []byte {
	_, hlen, blen := toytlv.ProbeHeader(tlv)
	return tlv[hlen+4 : hlen+blen]
}

func LWWtime(rec []byte) uint32 {
	if len(rec) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(rec[:4])
}

func LWWparse(bulk []byte) (lit byte, time uint32, rec, rest []byte) {
	lit, hlen, blen := toytlv.ProbeHeader(bulk)
	time = binary.LittleEndian.Uint32(bulk[hlen : hlen+4])
	rec = bulk[:hlen+blen]
	rest = bulk[hlen+blen:]
	return
}

func LWWmerge(lit byte, tlvs [][]byte) (tlv []byte) {
	var win []byte
	var maxt uint32
	for _, rec := range tlvs {
		for len(rec) > 0 {
			l, hlen, blen := toytlv.ProbeHeader(rec)
			time := binary.LittleEndian.Uint32(rec[hlen : hlen+4])
			_ = l // fixme bad lit
			if time > maxt {
				maxt = time
				win = rec[:hlen+blen]
			}
			rec = rec[hlen+blen:]
		}
	}
	return win
}

func LWWdelta(old_tlv, new_tlv []byte) (tlv_delta []byte) {
	lit, ohlen, _ := toytlv.ProbeHeader(old_tlv)
	_, nhlen, _ := toytlv.ProbeHeader(new_tlv)
	time := binary.LittleEndian.Uint32(old_tlv[ohlen : ohlen+4])
	new_time := []byte{0, 0, 0, 0}
	binary.LittleEndian.PutUint32(new_time, time+1)
	tlv_delta = toytlv.Record(lit, new_time, new_tlv[nhlen+4:])
	return
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
	return toytlv.Record('I', time0, ZipInt64(i))
}

// convert a TLV value to a plain golang value
func Iplain(tlv []byte) int64 {
	zipped := LWWvalue(tlv)
	return UnzipInt64(zipped)
}

// merge TLV values
func Imerge(tlvs [][]byte) (tlv []byte) {
	return LWWmerge('I', tlvs)
}

// produce an op that turns the old value into the new one
func Idelta(old_tlv, new_tlv []byte) (tlv_delta []byte) {
	return LWWdelta(old_tlv, new_tlv)
}

// checks a TLV value for validity (format violations)
func Ivalid(tlv []byte) bool {
	if len(tlv) < 4 {
		return false
	}
	return true
}

// S is a last-write-wins UTF-8 string

const hex = "0123456789abcdef"

// produce a text form (for REPL mostly)
func Sstring(tlv []byte) (txt string) {
	dst := make([]byte, 0, len(tlv)*2)
	val := LWWvalue(tlv)
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
	return toytlv.Record('S', time0, unesc)
}

// convert plain golang value into a TLV form
func Stlv(s string) (tlv []byte) {
	return toytlv.Record('S', time0, []byte(s))
}

// convert a TLV value to a plain golang value
func Splain(tlv []byte) string {
	return string(LWWvalue(tlv))
}

// merge TLV values
func Smerge(tlvs [][]byte) (tlv []byte) {
	return LWWmerge('S', tlvs)
}

// produce an op that turns the old value into the new one
func Sdelta(old_tlv, new_tlv []byte) (tlv_delta []byte) {
	return LWWdelta(old_tlv, new_tlv)
}

// checks a TLV value for validity (format violations)
func Svalid(tlv []byte) bool {
	if len(tlv) < 4 {
		return false
	}
	return true
}

// R is a last-write-wins ID

// produce a text form (for REPL mostly)
func Rstring(tlv []byte) (txt string) {
	return Rplain(tlv).String()
}

// parse a text form into a TLV value
func Rparse(txt string) (tlv []byte) {
	id := ParseID([]byte(txt))
	return Rtlv(id)
}

// convert plain golang value into a TLV form
func Rtlv(i ID) (tlv []byte) {
	return toytlv.Record('R', time0, ZipID(i))
}

// convert a TLV value to a plain golang value
func Rplain(tlv []byte) ID {
	return UnzipID(LWWvalue(tlv))
}

// merge TLV values
func Rmerge(tlvs [][]byte) (tlv []byte) {
	return LWWmerge('R', tlvs)
}

// produce an op that turns the old value into the new one
func Rdelta(old_tlv, new_tlv []byte) (tlv_delta []byte) {
	return LWWdelta(old_tlv, new_tlv)
}

// checks a TLV value for validity (format violations)
func Rvalid(tlv []byte) bool {
	if len(tlv) < 4 {
		return false
	}
	// todo correct sizes
	return true
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
	return toytlv.Record('F', time0, ZipUint64(math.Float64bits(i)))
}

// convert a TLV value to a plain golang value
func Fplain(tlv []byte) float64 {
	bits := UnzipUint64(LWWvalue(tlv))
	return math.Float64frombits(bits)
}

// merge TLV values
func Fmerge(tlvs [][]byte) (tlv []byte) {
	return LWWmerge('F', tlvs)
}

// produce an op that turns the old value into the new one
func Fdelta(old_tlv, new_tlv []byte) (tlv_delta []byte) {
	return LWWdelta(old_tlv, new_tlv)
}

// checks a TLV value for validity (format violations)
func Fvalid(tlv []byte) bool {
	if len(tlv) < 4 {
		return false
	}
	// todo do we accept NaN?
	return true
}
