package rdx

import (
	hex2 "encoding/hex"
	"github.com/learn-decentralized-systems/toytlv"
)

func Xparse(rdt byte, val string) (tlv []byte) {
	switch rdt {
	case 'F':
		tlv = Fparse(val)
	case 'I':
		tlv = Iparse(val)
	case 'R':
		tlv = Rparse(val)
	case 'S':
		tlv = Sparse(val)
	case 'T':
		tlv = Tparse(val)
	case 'N':
		tlv = Nparse(val)
	case 'Z':
		tlv = Zparse(val)
	}
	return
}

func Xmerge(rdt byte, tlvs [][]byte) (tlv []byte) {
	switch rdt {
	case 'C', 'O', 'Y': // object's ref is immutable
		tlv = COLAmerge(tlvs)
	case 'F':
		tlv = Fmerge(tlvs)
	case 'I':
		tlv = Imerge(tlvs)
	case 'R':
		tlv = Rmerge(tlvs)
	case 'S':
		tlv = Smerge(tlvs)
	case 'T':
		tlv = Tmerge(tlvs)
	case 'N':
		tlv = Nmerge(tlvs)
	case 'Z':
		tlv = Zmerge(tlvs)
	case 'V':
		tlv = Vmerge(tlvs)
	default:
		tlv = NoMerge(tlvs)
	}
	return
}

func Xstring(rdt byte, tlv []byte) string {
	switch rdt {
	case 'C', 'O', 'Y':
		return COLAstring(tlv)
	case 'F':
		return Fstring(tlv)
	case 'I':
		return Istring(tlv)
	case 'R':
		return Rstring(tlv)
	case 'S':
		return Sstring(tlv)
	case 'T':
		return Tstring(tlv)
	case 'N':
		return Nstring(tlv)
	default:
		hex := make([]byte, len(tlv)*2)
		hex2.Encode(hex, tlv)
		return string(hex)
	}

}

func Xdefault(rdt byte) (tlv []byte) {
	switch rdt {
	case 'C', 'O', 'Y':
		return COYdefault()
	case 'F', 'I', 'R', 'S', 'T':
		return FIRSTdefault(rdt)
	case 'N':
		return Ndefault()
	case 'E', 'L', 'M':
		return ELMdefault()
	default:
		return nil
	}
}

func X2string(rdt byte, tlv []byte, new_val string, src uint64) (delta []byte) {
	switch rdt {
	case 'C', 'O', 'Y':
		delta = nil
	case 'N':
		delta = N2string(tlv, new_val, src)
	default:
		delta = nil
	}
	return
}

func Xdiff(rdt byte, tlv []byte, sendvv VV) (diff []byte) {
	switch rdt {
	case 'C', 'O', 'Y':
		diff = nil
	case 'F':
		diff = Fdiff(tlv, sendvv)
	case 'I':
		diff = Idiff(tlv, sendvv)
	case 'R':
		diff = Rdiff(tlv, sendvv)
	case 'S':
		diff = Sdiff(tlv, sendvv)
	case 'T':
		diff = Tdiff(tlv, sendvv)
	case 'N':
		diff = Ndiff(tlv, sendvv)
	default:
		diff = tlv
	}
	return
}

func COYdefault() []byte {
	return ID0.ZipBytes()
}

func COLAmerge(inputs [][]byte) []byte {
	return inputs[0]
}

func COLAstring(tlv []byte) string {
	return IDFromZipBytes(tlv).String()
}

func NoMerge(inputs [][]byte) []byte {
	ret := make([]byte, 0, toytlv.TotalLen(inputs))
	for _, input := range inputs {
		ret = append(ret, input...)
	}
	return ret
}
