package replication

import (
	"github.com/drpcorg/chotki/chotki_errors"
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

func ParseHandshake(body []byte) (mode SyncMode, vv rdx.VV, trace_id []byte, err error) {
	// handshake: H(T{pro,src} M(mode) V(V{p,s}+), T(trace_id) ...)
	var mbody, vbody []byte
	rest := body
	mbody, rest = protocol.Take('M', rest)
	if mbody == nil {
		return 0, nil, nil, chotki_errors.ErrBadHPacket
	}

	vbody, rest = protocol.Take('V', rest)
	if vbody == nil {
		return 0, nil, nil, chotki_errors.ErrBadHPacket
	}

	vv = make(rdx.VV)
	if err := vv.PutTLV(vbody); err != nil {
		return 0, nil, nil, err
	}

	if err := mode.Unzip(mbody); err != nil {
		return 0, nil, nil, err
	}

	trace_id, _ = protocol.Take('S', rest)
	if trace_id == nil {
		return 0, nil, nil, chotki_errors.ErrBadHPacket
	}
	return mode, vv, trace_id, nil
}
