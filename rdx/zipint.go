package rdx

import (
	"encoding/binary"
	"math"
	"math/bits"
)

const Bytes4 = 0xffffffff
const Bytes2 = 0xffff
const Bytes1 = 0xff

func byteLen(n uint64) int {
	if n <= Bytes1 {
		if n == 0 {
			return 0
		}
		return 1
	}
	if n <= Bytes2 {
		return 2
	}
	if n <= Bytes4 {
		return 4
	}
	return 8
}

// ZipUint64Pair packs a pair of uint64 into a byte string.
// The smaller the ints, the shorter the string TODO 4+3 etc
func ZipUint64Pair(big, lil uint64) []byte {
	var ret = [16]byte{}
	pat := (byteLen(big) << 4) | byteLen(lil)
	switch pat {
	case 0x00:
		return ret[0:0]
	case 0x10:
		ret[0] = byte(big)
		return ret[0:1]
	case 0x01, 0x11: // 2
		ret[0] = byte(big)
		ret[1] = byte(lil)
		return ret[0:2]
	case 0x20, 0x21:
		binary.LittleEndian.PutUint16(ret[0:2], uint16(big))
		ret[2] = byte(lil)
		return ret[0:3]
	case 0x02, 0x12, 0x22:
		binary.LittleEndian.PutUint16(ret[0:2], uint16(big))
		binary.LittleEndian.PutUint16(ret[2:4], uint16(lil))
		return ret[0:4]
	case 0x40, 0x41:
		binary.LittleEndian.PutUint32(ret[0:4], uint32(big))
		ret[4] = byte(lil)
		return ret[0:5]
	case 0x42:
		binary.LittleEndian.PutUint32(ret[0:4], uint32(big))
		binary.LittleEndian.PutUint16(ret[4:6], uint16(lil))
		return ret[0:6]
	case 0x04, 0x14, 0x24, 0x44:
		binary.LittleEndian.PutUint32(ret[0:4], uint32(big))
		binary.LittleEndian.PutUint32(ret[4:8], uint32(lil))
		return ret[0:8]
	case 0x80, 0x81:
		binary.LittleEndian.PutUint64(ret[0:8], big)
		ret[8] = byte(lil)
		return ret[0:9]
	case 0x82:
		binary.LittleEndian.PutUint64(ret[0:8], big)
		binary.LittleEndian.PutUint16(ret[8:10], uint16(lil))
		return ret[0:10]
	case 0x84:
		binary.LittleEndian.PutUint64(ret[0:8], big)
		binary.LittleEndian.PutUint32(ret[8:12], uint32(lil))
		return ret[0:12]
	case 0x08, 0x18, 0x28, 0x48, 0x88:
		binary.LittleEndian.PutUint64(ret[0:8], big)
		binary.LittleEndian.PutUint64(ret[8:16], lil)
		return ret[0:16]
	}
	return ret[:]
}

func UnzipUint64Pair(buf []byte) (big, lil uint64) {
	switch len(buf) {
	case 0:
		return 0, 0
	case 1:
		return uint64(buf[0]), 0
	case 2:
		return uint64(buf[0]), uint64(buf[1])
	case 3:
		big = uint64(binary.LittleEndian.Uint16(buf[0:2]))
		lil = uint64(buf[2])
	case 4:
		big = uint64(binary.LittleEndian.Uint16(buf[0:2]))
		lil = uint64(binary.LittleEndian.Uint16(buf[2:4]))
	case 5:
		big = uint64(binary.LittleEndian.Uint32(buf[0:4]))
		lil = uint64(buf[4])
	case 6:
		big = uint64(binary.LittleEndian.Uint32(buf[0:4]))
		lil = uint64(binary.LittleEndian.Uint16(buf[4:6]))
	case 8:
		big = uint64(binary.LittleEndian.Uint32(buf[0:4]))
		lil = uint64(binary.LittleEndian.Uint32(buf[4:8]))
	case 9:
		big = uint64(binary.LittleEndian.Uint64(buf[0:8]))
		lil = uint64(buf[8])
	case 10:
		big = binary.LittleEndian.Uint64(buf[0:8])
		lil = uint64(binary.LittleEndian.Uint16(buf[8:10]))
	case 12:
		big = binary.LittleEndian.Uint64(buf[0:8])
		lil = uint64(binary.LittleEndian.Uint32(buf[8:12]))
	case 16:
		big = binary.LittleEndian.Uint64(buf[0:8])
		lil = binary.LittleEndian.Uint64(buf[8:16])
	default:
		// error!
	}
	return
}

func UnzipUint32Pair(buf []byte) (big, lil uint32) {
	switch len(buf) {
	case 0:
		return 0, 0
	case 1:
		return uint32(buf[0]), 0
	case 2:
		return uint32(buf[0]), uint32(buf[1])
	case 3:
		big = uint32(binary.LittleEndian.Uint16(buf[0:2]))
		lil = uint32(buf[2])
	case 4:
		big = uint32(binary.LittleEndian.Uint16(buf[0:2]))
		lil = uint32(binary.LittleEndian.Uint16(buf[2:4]))
	case 5:
		big = uint32(binary.LittleEndian.Uint32(buf[0:4]))
		lil = uint32(buf[4])
	case 6:
		big = uint32(binary.LittleEndian.Uint32(buf[0:4]))
		lil = uint32(binary.LittleEndian.Uint16(buf[4:6]))
	case 8:
		big = uint32(binary.LittleEndian.Uint32(buf[0:4]))
		lil = uint32(binary.LittleEndian.Uint32(buf[4:8]))
	default:
		// error!
	}
	return
}

func Uint32Pair(a, b uint32) (x uint64) {
	return uint64(a) | (uint64(b) << 32)
}

func Uint32Unpair(x uint64) (a, b uint32) {
	return uint32(x), uint32(x >> 32)
}

// ZipUint64 packs uint64 into a shortest possible byte string
func ZipUint64(v uint64) []byte {
	buf := [8]byte{}
	i := 0
	for v > 0 {
		buf[i] = uint8(v)
		v >>= 8
		i++
	}
	return buf[0:i]
}

func UnzipUint64(zip []byte) (v uint64) {
	for i := len(zip) - 1; i >= 0; i-- {
		v <<= 8
		v |= uint64(zip[i])
	}
	return
}

func ZigZagInt64(i int64) uint64 {
	return uint64(i*2) ^ uint64(i>>63)
}

func ZagZigUint64(u uint64) int64 {
	half := u >> 1
	mask := -(u & 1)
	return int64(half ^ mask)
}

func ZipZagInt64(i int64) []byte {
	return ZipUint64(ZigZagInt64(i))
}

func ZipInt64(v int64) []byte {
	return ZipUint64(ZigZagInt64(v))
}

func UnzipInt64(zip []byte) int64 {
	return ZagZigUint64(UnzipUint64(zip))
}

func ZipFloat64(f float64) []byte {
	fb := math.Float64bits(f)
	b := bits.Reverse64(fb)
	return ZipUint64(b)
}

func UnzipFloat64(zip []byte) float64 {
	b := UnzipUint64(zip)
	return math.Float64frombits(bits.Reverse64(b))
}

func ZipIntUint64Pair(i int64, u uint64) []byte {
	z := ZigZagInt64(i)
	return ZipUint64Pair(z, u)
}

func UnzipIntUint64Pair(zip []byte) (i int64, u uint64) {
	var z uint64
	z, u = UnzipUint64Pair(zip)
	i = ZagZigUint64(z)
	return
}
