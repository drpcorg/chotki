package chotki

import (
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/pkg/errors"
	"unicode/utf8"
)

var ErrBadTypeDescription = errors.New("bad type description")

func hasUnsafeChars(text string) bool {
	for _, l := range text {
		if l < ' ' {
			return true
		}
	}
	return false
}

var ErrUnknownObject = errors.New("unknown object")
var ErrTypeUnknown = errors.New("unknown object type")
var ErrUnknownFieldsInAType = errors.New("unknown fields for the type")
var ErrBadValueForAType = errors.New("bad value for the type")

func (ch *Chotki) ObjectType(tid ID) (formula string, err error) {
	formula, ok := ch.types[tid]
	if ok {
		return
	}
	f := []byte{}
	i := ch.ObjectIterator(tid)
	if i == nil {
		return "", ErrTypeUnknown
	}
	for ; i.Valid(); i.Next() {
		if len(i.Value()) > 0 {
			f = append(f, i.Value()[0])
		}
	}
	formula = string(f)
	ch.lock.Lock()
	ch.types[tid] = formula
	ch.lock.Unlock()
	return
}

func (ch *Chotki) CreateType(parent ID, fields ...string) (id ID, err error) {
	var fspecs toyqueue.Records
	fspecs = append(fspecs, toytlv.Record('A', parent.ZipBytes()))
	for _, field := range fields {
		if len(field) < 2 || field[0] < 'A' || field[0] > 'Z' || !utf8.ValidString(field) || hasUnsafeChars(field) {
			return BadId, ErrBadTypeDescription
		}
		fspecs = append(fspecs, toytlv.Record('A', []byte(field)))
	}
	return ch.CommitPacket('T', ID0, fspecs)
}

func (ch *Chotki) CreateObject(tid ID, fields ...string) (id ID, err error) {
	var formula string
	formula, err = ch.ObjectType(tid)
	if err != nil {
		return
	}
	if len(fields) > len(formula) {
		return BadId, ErrUnknownFieldsInAType
	}
	var packet toyqueue.Records
	for i := 0; i < len(fields); i++ {
		var tlv []byte
		switch formula[i] {
		case 'I':
			tlv = Iparse(fields[i])
		case 'S':
			tlv = Sparse(fields[i])
		case 'F':
			tlv = Fparse(fields[i])
		case 'R':
			tlv = Rparse(fields[i])
		}
		if tlv == nil {
			return BadId, ErrBadValueForAType
		}
		packet = append(packet, toytlv.Record(formula[i], tlv))
	}
	return ch.CommitPacket('O', tid, packet)
}

func (ch *Chotki) GetObject(oid ID) (tid ID, fields []string, err error) {
	i := ch.ObjectIterator(oid)
	if i == nil || !i.Valid() {
		return BadId, nil, ErrUnknownObject
	}
	tid = IDFromZipBytes(i.Value())
	for i.Next() {
		_, rdt := OKeyIdRdt(i.Key())
		var str string
		switch rdt {
		case 'I':
			str = Istring(i.Value())
		case 'S':
			str = Sstring(i.Value())
		case 'F':
			str = Fstring(i.Value())
		case 'R':
			str = Rstring(i.Value())
		}
		fields = append(fields, str)
	}
	return
}
