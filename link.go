package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toytlv"
)

func LState(link rdx.ID, time uint64) []byte {
	return toytlv.Record('L',
		toytlv.Record('T', rdx.ZipUint64(time)),
		link.ZipBytes(),
	)
}
