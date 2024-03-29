package chotki

import (
	hex2 "encoding/hex"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
)

func ChotkiKVString(key, value []byte) string {
	if len(key) != LidLKeyLen {
		return ""
	}
	line := make([]byte, 0, 128)
	//line = append(line, key[0], '.')
	id, rdt := OKeyIdRdt(key)
	line = append(line, id.String()...)
	line = append(line, '.', byte(rdt), ':', '\t')
	switch rdt {
	case 'A':
		ref := IDFromZipBytes(value)
		line = append(line, ref.String()...)
	case 'I':
		line = append(line, Istring(value)...)
	case 'S':
		line = append(line, Sstring(value)...)
	case 'R':
		line = append(line, Rstring(value)...)
	case 'F':
		line = append(line, Fstring(value)...)
	default:
		hex := make([]byte, len(value)*2)
		hex2.Encode(hex, value)
		line = append(line, hex...)
	}
	return string(line)
}

func (ch *Chotki) DumpAll() {
	_, _ = fmt.Fprintf(os.Stderr, "=====OBJECTS=====\n")
	ch.DumpObjects()
	_, _ = fmt.Fprintf(os.Stderr, "=====VVS=====\n")
	ch.DumpVV()
}

func (ch *Chotki) DumpObjects() {
	io := pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	}
	i := ch.db.NewIter(&io)
	for i.SeekGE([]byte{'O'}); i.Valid(); i.Next() {
		_, _ = fmt.Fprintln(os.Stderr, ChotkiKVString(i.Key(), i.Value()))
	}
}

func (ch *Chotki) DumpVV() {
	io := pebble.IterOptions{
		LowerBound: []byte{'V'},
		UpperBound: []byte{'W'},
	}
	i := ch.db.NewIter(&io)
	for i.SeekGE(VKey(ID0)); i.Valid(); i.Next() {
		id := IDFromBytes(i.Key()[1:])
		vv := make(VV)
		_ = vv.PutTLV(i.Value())
		fmt.Printf("%s -> \t%s\n", id.String(), vv.String())
	}
}

func DumpVPacket(vvs map[ID]VV) {
	for id, vv := range vvs {
		_, _ = fmt.Fprintf(os.Stderr, "%s -> %s\n", id.String(), vv.String())
	}
}
