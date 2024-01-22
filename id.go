package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/learn-decentralized-systems/toytlv"
)

// ID is an 64-bit locator/identifier that contains:
//   - op seq number (30 bits),
//   - offset (10 bits), and
//   - the replica id (24 bits).
//
// This is NOT a Lamport timestamp (need more bits for that).
type ID uint64

const SeqBits = 32
const OffBits = 12
const SrcBits = 20
const SeqOffBits = SeqBits + OffBits
const SeqOffMask = uint64(uint64(1)<<SeqOffBits) - 1
const OffMask = uint64(1<<OffBits) - 1
const FieldTypeBits = 5
const FieldNoBits = OffBits - FieldTypeBits
const FieldTypeMask = (uint16(1) << FieldTypeBits) - 1

const BadId = ID(uint64(0xfff) << SeqOffBits)

func MakeID(src uint32, seq uint32, off uint16) ID {
	ret := uint64(src)
	ret <<= SeqBits
	ret |= uint64(seq)
	ret <<= OffBits
	ret |= uint64(off)
	return ID(ret)
}

func SrcSeqOff(id ID) (src uint32, seq uint32, off uint16) {
	n := uint64(id)
	off = uint16(n & OffMask)
	n >>= OffBits
	seq = uint32(n)
	n >>= SeqBits
	src = uint32(n)
	return
}

func MakeField(rdt byte, field byte) (off uint16) {
	if rdt < 'A' || rdt > 'Z' {
		panic("bad RDT lit")
	}
	off = uint16(field)
	off <<= FieldTypeBits
	off |= uint16(rdt - 'A')
	return
}

func FieldNameType(off uint16) (field, rdt byte) {
	rdt = byte((off & FieldTypeMask) + 'A')
	field = byte(off >> FieldTypeBits)
	return
}

// Seq is the op sequence number (each replica generates its own
// sequence numbers)
func (id ID) Seq() uint32 {
	i := uint64(id)
	return uint32((i & SeqOffMask) >> OffBits)
}

func (id ID) Off() uint16 {
	return uint16(uint64(id) & OffMask)
}

func (id ID) ToOff(newoff uint16) ID {
	return ID((uint64(id) & ^OffMask) | uint64(newoff))
}

// Src is the replica id. That is normally a small number.
func (id ID) Src() uint32 {
	return uint32(id >> SeqOffBits)
}

func (id ID) Bytes() []byte {
	var ret [8]byte
	binary.LittleEndian.PutUint64(ret[:], uint64(id))
	return ret[:]
}

func (id ID) ZipBytes() []byte {
	i := uint64(id)
	return ZipUint64Pair(i&SeqOffMask, i>>SeqOffBits)
}

func ZipID(id ID) (zip []byte) {
	i := uint64(id)
	return ZipUint64Pair(i&SeqOffMask, i>>SeqOffBits)
}

func UnzipID(zip []byte) ID {
	big, lil := UnzipUint64Pair(zip)
	return ID(big | (lil << SeqOffBits))
}

func (id ID) Feed(into []byte) (res []byte) {
	pair := id.ZipBytes()
	res = toytlv.AppendHeader(into, 'I', len(pair))
	res = append(res, pair...)
	return res
}

func (id *ID) Drain(from []byte) (rest []byte) { // FIXME
	body, rest := toytlv.Take('I', from)
	seq, orig := UnzipUint64Pair(body)
	*id = ID((seq << SrcBits) | orig)
	return rest
}

var ErrBadId = errors.New("not an expected id")

func TakeIDWary(lit byte, pack []byte) (id ID, rest []byte, err error) {
	idbytes, rest := toytlv.Take(lit, pack)
	if idbytes == nil {
		err = ErrBadId
	} else {
		id = UnzipID(idbytes)
	}
	return
}

func (id ID) String() string {
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

func (id ID) Hex583(ret []byte) []byte {
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
	return append(ret, hex...)
}

func (id ID) String583() string {
	return string(id.Hex583(nil))
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

func ParseID(id string) ID {
	if len(id) > 16+2 {
		return BadId
	}
	var parts [3]uint64
	i, p := 0, 0
	for i < len(id) && p < 3 {
		c := id[i]
		if c >= '0' && c <= '9' {
			parts[p] = (parts[p] << 4) | uint64(c-'0')
		} else if c >= 'A' && c <= 'F' {
			parts[p] = (parts[p] << 4) | uint64(10+c-'A')
		} else if c >= 'a' && c <= 'f' {
			parts[p] = (parts[p] << 4) | uint64(10+c-'a')
		} else if c == '-' {
			p++
		} else {
			return BadId
		}
		i++
	}
	switch p {
	case 0: // off
		parts[2] = parts[0]
		parts[0] = 0
	case 1: // src-seq
	case 2: // src-seq-off
	case 3:
		return BadId
	}
	if parts[1] > 0xffffffff || parts[2] > 0xfff || parts[0] > 0xfffff {
		return BadId
	}
	return MakeID(uint32(parts[0]), uint32(parts[1]), uint16(parts[2]))
}
