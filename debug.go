package chotki

import (
	hex2 "encoding/hex"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
	"os"
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
		ref := rdx.IDFromZipBytes(value)
		line = append(line, ref.String()...)
	case 'I':
		line = append(line, rdx.Istring(value)...)
	case 'S':
		line = append(line, rdx.Sstring(value)...)
	case 'R', 'T':
		line = append(line, rdx.Rstring(value)...)
	case 'F':
		line = append(line, rdx.Fstring(value)...)
	default:
		hex := make([]byte, len(value)*2)
		hex2.Encode(hex, value)
		line = append(line, hex...)
	}
	return string(line)
}

func (cho *Chotki) DumpAll() {
	_, _ = fmt.Fprintf(os.Stderr, "=====OBJECTS=====\n")
	cho.DumpObjects()
	_, _ = fmt.Fprintf(os.Stderr, "=====VVS=====\n")
	cho.DumpVV()
}

func (cho *Chotki) DumpObjects() {
	io := pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	}
	i := cho.db.NewIter(&io)
	for i.SeekGE([]byte{'O'}); i.Valid(); i.Next() {
		_, _ = fmt.Fprintln(os.Stderr, ChotkiKVString(i.Key(), i.Value()))
	}
}

func (cho *Chotki) DumpVV() {
	io := pebble.IterOptions{
		LowerBound: []byte{'V'},
		UpperBound: []byte{'W'},
	}
	i := cho.db.NewIter(&io)
	for i.SeekGE(VKey(rdx.ID0)); i.Valid(); i.Next() {
		id := rdx.IDFromBytes(i.Key()[1:])
		vv := make(rdx.VV)
		_ = vv.PutTLV(i.Value())
		fmt.Printf("%s -> \t%s\n", id.String(), vv.String())
	}
}

func DumpVPacket(vvs map[rdx.ID]rdx.VV) {
	for id, vv := range vvs {
		_, _ = fmt.Fprintf(os.Stderr, "%s -> %s\n", id.String(), vv.String())
	}
}
