package main

import (
	"bytes"
	"github.com/learn-decentralized-systems/toytlv"
)

type String string

func (s String) Len() int {
	return len(s)
}

func parseS(state []byte) (str []byte, t uint64) {
	_, rest := toytlv.Take('I', state)
	body, rest := toytlv.Take('S', rest)
	tbuf, str := toytlv.Take('T', body)
	t = UnzipUint64(tbuf)
	return str, t
}

func (s *String) Apply(state []byte) {
	str, _ := parseS(state)
	(*s) = String(str)
}

func (s *String) Diff(id ID, state []byte) (changes []byte) {
	str, time := parseS(state)
	if 0 != bytes.Compare(str, []byte(*s)) {
		changes = toytlv.Concat(
			toytlv.Record('I', id.ZipBytes()),
			toytlv.Record('S',
				toytlv.Record('T', ZipUint64(time)),
				[]byte(*s),
			),
		)
	}
	return
}

func SMerge(inputs [][]byte) []byte {
	return LMerge(inputs)
}
