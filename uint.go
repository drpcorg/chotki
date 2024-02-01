package main

import (
	"github.com/learn-decentralized-systems/toytlv"
)

type Uint64 uint64

func UState(u, time uint64) []byte {
	return toytlv.Record('U',
		toytlv.Record('T', ZipUint64(time)),
		ZipUint64(u),
	)
}

func ParseUState(state []byte) (u, time uint64) {
	body, _ := toytlv.Take('U', state)
	tbuf, ubuf := toytlv.Take('T', body)
	time = UnzipUint64(tbuf)
	u = UnzipUint64(ubuf)
	return
}

func parseU(state []byte) (u uint64, t uint64) {
	rest, _ := toytlv.Take('I', state)
	body, rest := toytlv.Take('S', rest)
	tbuf, ubuf := toytlv.Take('T', body)
	t = UnzipUint64(tbuf)
	u = UnzipUint64(ubuf)
	return u, t
}

func (s *Uint64) Apply(state []byte) {
	val, _ := ParseUState(state)
	// FIXME t
	(*s) = Uint64(val)
}

func (s *Uint64) Diff(state []byte) (changes []byte) {
	old, time := parseU(state)
	if old != uint64(*s) {
		changes = toytlv.Concat(
			//toytlv.Record('I', id.ZipBytes()), // FIXME rly?
			toytlv.Record('U',
				toytlv.Record('T', ZipUint64(time)),
				ZipUint64(uint64(*s)),
			),
		)
	}
	return
}

func UMerge(inputs [][]byte) []byte {
	return LMerge(inputs)
}
