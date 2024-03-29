package rdx

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/learn-decentralized-systems/toytlv"
)

/*
	ID is an 64-bit locator/identifier.
	This is NOT a Lamport timestamp (need more bits for that).
	This is *log time*, not *logical time*.

0...............16..............32..............48.............64
+-------+-------+-------+-------+-------+-------+-------+-------
|offset(12)||......sequence.(32.bits)......||.source.(20.bits).|
|...........progress.(44.bits).............|....................
*/
type ID uint64

const ID0 ID = 0

const SeqBits = 32
const OffBits = 12
const SrcBits = 20
const ProBits = SeqBits + OffBits
const ProMask = uint64(uint64(1)<<ProBits) - 1
const OffMask = ID(1<<OffBits) - 1
const ProInc = ID(1 << OffBits)
const MaxSrc = (1 << SrcBits) - 1

const SeqOne = 1 << OffBits
const BadId = ID(0xffffffffffffffff)
const ZeroId = ID(0)

func IDfromSrcPro(src, pro uint64) ID {
	return ID((src << ProBits) | pro)
}

func IDFromSrcSeqOff(src uint64, seq uint64, off uint16) ID {
	ret := uint64(src)
	ret <<= SeqBits
	ret |= uint64(seq)
	ret <<= OffBits
	ret |= uint64(off)
	return ID(ret)
}

func SrcSeqOff(id ID) (src uint64, seq uint64, off uint16) {
	off = uint16(id & OffMask)
	id >>= OffBits
	seq = uint64(id)
	id >>= SeqBits
	src = uint64(id)
	return
}

func (id ID) ZeroOff() ID {
	return id & ID(^OffMask)
}

// Seq is the op sequence number (each replica generates its own
// sequence numbers)
func (id ID) Seq() uint64 {
	i := uint64(id)
	return (i & ProMask) >> OffBits
}

func (id ID) Pro() uint64 {
	i := uint64(id)
	return i & ProMask
}

func (id ID) Off() uint16 {
	return uint16(id & OffMask)
}

func (id ID) ToOff(newoff uint16) ID {
	return (id & ^OffMask) | ID(newoff)
}

// Src is the replica id. That is normally a small number.
func (id ID) Src() uint64 {
	return uint64(id >> ProBits)
}

func (id ID) Bytes() []byte {
	var ret [8]byte
	binary.BigEndian.PutUint64(ret[:], uint64(id))
	return ret[:]
}

func IDFromBytes(by []byte) ID {
	return ID(binary.BigEndian.Uint64(by))
}

func (id ID) ZipBytes() []byte {
	return ZipUint64Pair(id.Pro(), id.Src())
}

func IDFromZipBytes(zip []byte) ID {
	big, lil := UnzipUint64Pair(zip) // todo range check
	return ID(big | (lil << ProBits))
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
		id = IDFromZipBytes(idbytes)
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

func (id ID) Hex583() []byte {
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

func (id ID) String583() string {
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

func ParseIDString(id string) ID {
	return IDFromString([]byte(id))
}

func ParseBracketedID(bid []byte) ID {
	if len(bid) < 7 || bid[0] != '{' || bid[len(bid)-1] != '}' {
		return BadId
	}
	return IDFromString(bid[1 : len(bid)-1])
}

func IDFromString(idstr []byte) (parsed ID) {
	parsed, _ = readIDFromString(idstr)
	return
}

func readIDFromString(idstr []byte) (ID, []byte) {
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
	rest := idstr[i:]

	switch p {
	case 0: // off
		parts[2] = parts[0]
		parts[0] = 0
	case 1: // src-seq
	case 2: // src-seq-off
	case 3:
		return BadId, rest
	}

	if parts[1] > 0xffffffff || parts[2] > 0xfff || parts[0] > 0xfffff {
		return BadId, rest
	}

	return IDFromSrcSeqOff(parts[0], parts[1], uint16(parts[2])), rest
}

func readIDFromTLV(tlv []byte) (ID, []byte) { //nolint:golint,unused
	lit, body, rest := toytlv.TakeAny(tlv)
	if lit == '-' || lit == 0 {
		return BadId, nil
	}
	return IDFromZipBytes(body), rest
}
