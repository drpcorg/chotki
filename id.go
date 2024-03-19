package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/learn-decentralized-systems/toytlv"
)

/*
		id64 is an 64-bit locator/identifier.
	    This is NOT a Lamport timestamp (need more bits for that).

0...............16..............32..............48.............64
+-------+-------+-------+-------+-------+-------+-------+-------
|offset(12)||......sequence.(32.bits)......||.source.(20.bits).|
|...........progress.(44.bits).............|....................
|rdt||field|....................................................
*/
type id64 uint64

const ID0 id64 = 0

const SeqBits = 32
const OffBits = 12
const SrcBits = 20
const ProBits = SeqBits + OffBits
const ProMask = uint64(uint64(1)<<ProBits) - 1
const OffMask = uint64(1<<OffBits) - 1
const RdtBits = 5
const RdtInc = 1 << RdtBits
const FNoBits = OffBits - RdtBits
const RdtMask = (uint16(1) << RdtBits) - 1

const SeqOne = 1 << OffBits
const BadId = id64(0xffffffffffffffff)
const ZeroId = id64(0)

func IDfromSrcPro(src, pro uint64) id64 {
	return id64((src << ProBits) | pro)
}

func IDFromSrcSeqOff(src uint64, seq uint64, off uint16) id64 {
	ret := uint64(src)
	ret <<= SeqBits
	ret |= uint64(seq)
	ret <<= OffBits
	ret |= uint64(off)
	return id64(ret)
}

func SrcSeqOff(id id64) (src uint64, seq uint64, off uint16) {
	n := uint64(id)
	off = uint16(n & OffMask)
	n >>= OffBits
	seq = n
	n >>= SeqBits
	src = n
	return
}

func MakeField(rdt byte, field byte) (off uint16) {
	if rdt < 'A' || rdt > 'Z' {
		panic("bad RDT lit")
	}
	off = uint16(field)
	off <<= RdtBits
	off |= uint16(rdt - 'A')
	return
}

func (id id64) FieldRDT() (field, rdt byte) {
	const FieldNoMask = (1 << FNoBits) - 1
	rdt = byte(uint16(id)&RdtMask) + 'A'
	field = uint8(id>>RdtBits) & FieldNoMask
	return
}

func (id id64) RDT() byte {
	return (byte(id) & RdtBits) + 'A'
}

func (id id64) ZeroOff() id64 {
	return id & id64(^OffMask)
}

func (id id64) FNo() (fno byte) {
	mask := id64(1<<FNoBits) - 1
	return byte((id >> RdtBits) & mask)
}

func FNoRdt(off uint16) (field, rdt byte) {
	rdt = byte((off & RdtMask) + 'A')
	field = byte(off >> RdtBits)
	return
}

// Seq is the op sequence number (each replica generates its own
// sequence numbers)
func (id id64) Seq() uint64 {
	i := uint64(id)
	return (i & ProMask) >> OffBits
}

func (id id64) Pro() uint64 {
	i := uint64(id)
	return i & ProMask
}

func (id id64) Off() uint16 {
	return uint16(uint64(id) & OffMask)
}

func (id id64) ToOff(newoff uint16) id64 {
	return id64((uint64(id) & ^OffMask) | uint64(newoff))
}

// Src is the replica id. That is normally a small number.
func (id id64) Src() uint64 {
	return uint64(id >> ProBits)
}

func (id id64) Bytes() []byte {
	var ret [8]byte
	binary.BigEndian.PutUint64(ret[:], uint64(id))
	return ret[:]
}

func IDFromBytes(by []byte) id64 {
	return id64(binary.BigEndian.Uint64(by))
}

func (id id64) ZipBytes() []byte {
	return ZipUint64Pair(id.Pro(), id.Src())
}

func IDFromZipBytes(zip []byte) id64 {
	big, lil := UnzipUint64Pair(zip) // todo range check
	return id64(big | (lil << ProBits))
}

func (id id64) Feed(into []byte) (res []byte) {
	pair := id.ZipBytes()
	res = toytlv.AppendHeader(into, 'I', len(pair))
	res = append(res, pair...)
	return res
}

func (id *id64) Drain(from []byte) (rest []byte) { // FIXME
	body, rest := toytlv.Take('I', from)
	seq, orig := UnzipUint64Pair(body)
	*id = id64((seq << SrcBits) | orig)
	return rest
}

var ErrBadId = errors.New("not an expected id")

func TakeIDWary(lit byte, pack []byte) (id id64, rest []byte, err error) {
	idbytes, rest := toytlv.Take(lit, pack)
	if idbytes == nil {
		err = ErrBadId
	} else {
		id = IDFromZipBytes(idbytes)
	}
	return
}

func (id id64) String() string {
	seq := id.Seq()
	off := id.Off()
	src := id.Src()
	if src == 0 && seq == 0 {
		return fmt.Sprintf("%x", off)
	} else if off == 0 {
		return fmt.Sprintf("%x-%x", src, seq)
	} else {
		return fmt.Sprintf("%x-%x-%x", src, seq, off)
	}
}

const Hex = "0123456789abcdef"

const Hex583Len = 18

func Parse583Off(hex583 []byte) (off uint16) {
	return uint16(UnHex(hex583[len(hex583)-3:]))
}

func (id id64) Hex583() []byte {
	hex := []byte{'0', '0', '0', '0', '0', '-', '0', '0', '0', '0', '0', '0', '0', '0', '-', '0', '0', '0'}
	k := Hex583Len - 1
	u := uint64(id)
	for k > 14 {
		hex[k] = Hex[u&0xf]
		u >>= 4
		k--
	}
	k--
	for k > 5 {
		hex[k] = Hex[u&0xf]
		u >>= 4
		k--
	}
	k--
	for k >= 0 {
		hex[k] = Hex[u&0xf]
		u >>= 4
		k--
	}
	return hex
}

func (id id64) String583() string {
	return string(id.Hex583())
}

func UnHex(hex []byte) (num uint64) {
	for len(hex) > 0 {
		c := hex[0]
		if c >= '0' && c <= '9' {
			num = (num << 4) | uint64(c-'0')
		} else if c >= 'A' && c <= 'F' {
			num = (num << 4) | uint64(10+c-'A')
		} else if c >= 'a' && c <= 'f' {
			num = (num << 4) | uint64(10+c-'a')
		} else {
			break
		}
		hex = hex[1:]
	}
	return
}

func ParseIDString(id string) id64 {
	return IDFromString([]byte(id))
}

func ParseBracketedID(bid []byte) id64 {
	if len(bid) < 7 || bid[0] != '{' || bid[len(bid)-1] != '}' {
		return BadId
	}
	return IDFromString(bid[1 : len(bid)-1])
}

func IDFromString(idstr []byte) (parsed id64) {
	parsed, _ = readIDFromString(idstr)
	return
}

func readIDFromString(idstr []byte) (id id64, rest []byte) {
	var parts [3]uint64
	i, p := 0, 0
	for i < len(idstr) && p < 3 {
		c := idstr[i]
		if c >= '0' && c <= '9' {
			parts[p] = (parts[p] << 4) | uint64(c-'0')
		} else if c >= 'A' && c <= 'F' {
			parts[p] = (parts[p] << 4) | uint64(10+c-'A')
		} else if c >= 'a' && c <= 'f' {
			parts[p] = (parts[p] << 4) | uint64(10+c-'a')
		} else if c == '-' {
			p++
		} else {
			break
		}
		i++
	}
	rest = idstr[i:]
	switch p {
	case 0: // off
		parts[2] = parts[0]
		parts[0] = 0
	case 1: // src-seq
	case 2: // src-seq-off
	case 3:
		id = BadId
	}
	if parts[1] > 0xffffffff || parts[2] > 0xfff || parts[0] > 0xfffff {
		id = BadId
	}
	return IDFromSrcSeqOff(parts[0], parts[1], uint16(parts[2])), rest
}

func readIDFromTLV(tlv []byte) (id id64, rest []byte) {
	lit, body, rest := toytlv.TakeAny(tlv)
	if lit == '-' || lit == 0 {
		return BadId, nil
	}
	id = IDFromZipBytes(body)
	return
}
