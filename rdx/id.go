package rdx

import (
	"encoding/binary"
	"strconv"

	"github.com/drpcorg/chotki/protocol"
)

/*
	ID is an 64-bit locator/identifier.
	This is NOT a Lamport timestamp (need more bits for that).
	This is *log time*, not *logical time*.

0...............16..............32..............48.............64
+-------+-------+-------+-------+-------+-------+-------+-------
|offset(12)||......sequence.(32.bits)......|..source.(20.bits)..|
|...........progress.(44.bits).............|....................|
*/
type ID struct {
	src uint64
	pro uint64
}

const proBits = 52
const proMask = uint64(uint64(1)<<proBits) - 1
const offBits = 12
const OffMask = uint64(1<<offBits) - 1
const proInc = 1 << offBits

func NewID(src uint64, seq uint64, offset uint64) ID {
	pro := seq<<offBits | offset
	return ID{src, pro}
}

func (id ID) ProAnd(mask uint64) ID {
	return ID{id.src, id.pro & mask}
}

func (id ID) ProOr(mask uint64) ID {
	return ID{id.src, id.pro | mask}
}
func (id ID) ProPlus(inc uint64) ID {
	return ID{id.src, id.pro + inc}
}

func (id ID) ProMinus(dec uint64) ID {
	return ID{id.src, id.pro - dec}
}

func (id ID) Less(other ID) bool {
	if id.src != other.src {
		return id.src < other.src
	}
	return id.pro < other.pro
}

var ID0 ID = ID{}

const MaxSrc = (1 << 32) - 1

var BadId = ID{^uint64(0), ^uint64(0)}

func IDfromSrcPro(src, pro uint64) ID {
	return ID{src, pro}
}

func IDFromSrcSeqOff(src uint64, seq uint64, off uint16) ID {
	pro := seq<<offBits | uint64(off)
	return ID{src, pro}
}

func SrcSeqOff(id ID) (src uint64, seq uint64, off uint16) {
	return id.Src(), id.Seq(), uint16(id.Off())
}

func (id ID) ZeroOff() ID {
	id.pro &= ^OffMask
	return id
}

// Seq is the op sequence number (each replica generates its own
// sequence numbers)
func (id ID) Seq() uint64 {
	return id.pro >> offBits
}

func (id ID) Pro() uint64 {
	return id.pro & proMask
}

func (id ID) Off() uint64 {
	return id.pro & OffMask
}

func (id ID) ToOff(newoff uint64) ID {
	return ID{id.src, id.pro & ^OffMask | newoff}
}

func (id ID) IncPro(inc uint64) ID {
	return ID{id.src, id.pro + proInc}
}

// Src is the replica id. That is normally a small number.
func (id ID) Src() uint64 {
	return uint64(id.src)
}

func (id ID) Bytes() []byte {
	var ret [16]byte
	binary.BigEndian.PutUint64(ret[:8], id.src)
	binary.BigEndian.PutUint64(ret[8:16], id.Pro())
	return ret[:]
}

func IDFromBytes(by []byte) ID {
	return ID{
		src: binary.BigEndian.Uint64(by[:8]),
		pro: binary.BigEndian.Uint64(by[8:16]),
	}
}

func (id ID) ZipBytes() []byte {
	return ZipUint64Pair(id.Src(), id.Pro())
}

func IDFromZipBytes(zip []byte) ID {
	big, lil := UnzipUint64Pair(zip) // todo range check
	return ID{
		src: big,
		pro: lil,
	}
}

func (id ID) String() string {

	var buf [64]byte
	b := buf[:0]

	b = strconv.AppendInt(b, int64(id.Src()), 16)
	b = append(b, '-')
	b = strconv.AppendInt(b, int64(id.Seq()), 16)

	if off := id.Off(); off != 0 {
		b = append(b, '-')
		b = strconv.AppendInt(b, int64(off), 16)
	}

	return string(b)

}

func IDFromString(idstr string) (parsed ID) {
	parsed, _ = readIDFromString([]byte(idstr))
	return
}

func IDFromText(idstr []byte) (parsed ID) {
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
	case 1: // Src-seq
	case 2: // Src-seq-off
	case 3:
		return BadId, rest
	}

	if parts[1] > 0xffffffff || parts[2] > 0xfff || parts[0] > 0xfffff {
		return BadId, rest
	}

	return IDFromSrcSeqOff(parts[0], parts[1], uint16(parts[2])), rest
}

func readIDFromTLV(tlv []byte) (ID, []byte) { //nolint:golint,unused
	lit, body, rest := protocol.TakeAny(tlv)
	if lit == '-' || lit == 0 {
		return BadId, nil
	}
	return IDFromZipBytes(body), rest
}
