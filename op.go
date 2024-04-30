package chotki

import (
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

func ParsePacket(pack []byte) (lit byte, id, ref rdx.ID, body []byte, err error) {
	lit, hlen, blen := protocol.ProbeHeader(pack)
	if lit == 0 || lit == '-' || hlen+blen > len(pack) {
		err = rdx.ErrBadPacket
		return
	}
	body = pack[hlen : hlen+blen]
	i, ihlen, iblen := protocol.ProbeHeader(body)
	if lit != 'D' && lit != 'V' && lit != 'H' {
		if i != 'I' && i != '0' {
			err = rdx.ErrBadPacket
			return
		}
	} else {
		if i != 'T' && i != '0' {
			err = rdx.ErrBadPacket
			return
		}
	}
	id = rdx.IDFromZipBytes(body[ihlen : ihlen+iblen])
	body = body[ihlen+iblen:]
	r, rhlen, rblen := protocol.ProbeHeader(body)
	if r == 'R' {
		ref = rdx.IDFromZipBytes(body[rhlen : rhlen+rblen])
		body = body[rhlen+rblen:]
	}
	return
}

// PacketSeqSrc picks the I field from the packet.
// Returns 0,0 if nothing found.
func PacketSrcSeq(pack []byte) (src, seq uint32) { // FIXME offset
	lit, hlen, blen := protocol.ProbeHeader(pack)
	if lit == 0 || hlen+blen > len(pack) {
		return
	}
	v, vhlen, vblen := protocol.ProbeHeader(pack[hlen:])
	if v != 'V' || vhlen+vblen > len(pack)-blen {
		return
	}
	big, lil := rdx.UnzipUint64Pair(pack[hlen+vhlen : hlen+vhlen+vblen])
	seq = uint32(big)
	src = uint32(lil)
	return
}

func PacketID(pack []byte) rdx.ID {
	lit, hlen, blen := protocol.ProbeHeader(pack)
	if lit == 0 || hlen+blen > len(pack) {
		return rdx.ZeroId
	}
	v, vhlen, vblen := protocol.ProbeHeader(pack[hlen:])
	if v != 'V' || vhlen+vblen > len(pack)-blen {
		return rdx.ZeroId
	}
	return rdx.IDFromZipBytes(pack[hlen+vhlen : hlen+vhlen+vblen])
}
