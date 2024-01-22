package main

import "github.com/learn-decentralized-systems/toytlv"

// PacketID picks the I field from the packet.
// Returns 0,0 if nothing found.
func PacketID(pack []byte) (seq, src uint32) {
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
