package main

import "github.com/learn-decentralized-systems/toytlv"

func ParsePacket(pack []byte) (lit byte, id, ref ID, body []byte, err error) {
	lit, hlen, blen := toytlv.ProbeHeader(pack)
	if lit == 0 || lit == '-' || hlen+blen > len(pack) {
		err = ErrBadPacket
		return
	}
	body = pack[hlen : hlen+blen]
	i, ihlen, iblen := toytlv.ProbeHeader(body)
	if i != 'I' {
		err = ErrBadPacket
		return
	}
	id = IDFromZipBytes(body[ihlen : ihlen+iblen])
	body = body[ihlen+iblen:]
	r, rhlen, rblen := toytlv.ProbeHeader(body)
	if r == 'R' {
		ref = IDFromZipBytes(body[rhlen : rhlen+rblen])
		body = body[rhlen+rblen:]
	}
	return
}

// PacketSeqSrc picks the I field from the packet.
// Returns 0,0 if nothing found.
func PacketSrcSeq(pack []byte) (src, seq uint32) { // FIXME offset
	lit, hlen, blen := toytlv.ProbeHeader(pack)
	if lit == 0 || hlen+blen > len(pack) {
		return
	}
	v, vhlen, vblen := toytlv.ProbeHeader(pack[hlen:])
	if v != 'V' || vhlen+vblen > len(pack)-blen {
		return
	}
	big, lil := UnzipUint64Pair(pack[hlen+vhlen : hlen+vhlen+vblen])
	seq = uint32(big)
	src = uint32(lil)
	return
}

func PacketID(pack []byte) ID {
	lit, hlen, blen := toytlv.ProbeHeader(pack)
	if lit == 0 || hlen+blen > len(pack) {
		return ZeroId
	}
	v, vhlen, vblen := toytlv.ProbeHeader(pack[hlen:])
	if v != 'V' || vhlen+vblen > len(pack)-blen {
		return ZeroId
	}
	return IDFromZipBytes(pack[hlen+vhlen : hlen+vhlen+vblen])
}
