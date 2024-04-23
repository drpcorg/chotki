package chotki

import (
	"fmt"
	"github.com/drpcorg/chotki/rdx"
)

var fragt = "type %s struct {\n"
var fragf = "\t%s\t%s\n"
var frage = `}
			var %sClassID = rdx.ID(%d)
			`
var fragl = `func (o *%s) Load(i *pebble.Iterator) (err error) {
				    for ; i.Valid(); i.Next() {
						id, rdt := chotki.OKeyIdRdt(i.Key())
						switch (id.Off()) {
						case 0:
							if rdt != 'O' { return chotki.ErrBadORecord }
							z := rdx.IDFromZipBytes(i.Value())
							if z != %sClassID { return chotki.ErrBadORecord }
			`
var fragn = `			case %d:
							if rdt != '%c' { break }
							o.%s = rdx.%cnative(i.Value())
			`
var fragu = `        }
            	}
            	return nil
			}
			`
var frags = `func (o *%s) Store(i *pebble.Iterator) (delta [][]byte, err error) {
				if i==nil {
			`
var fragi = "		delta = append(delta, rdx.%ctlv(o.%s))\n"
var fragd = `		return
				}
				had := uint64(0)
				for i.Next(); i.Valid(); i.Next() {
					id, rdt := chotki.OKeyIdRdt(i.Key())
					switch (id.Off()) {
					case 0:
						if rdt != 'O' { return nil, chotki.ErrBadORecord }
						z := rdx.IDFromZipBytes(i.Value())
						if z != %sClassID { return nil, chotki.ErrBadORecord }
			`
var fraga = `    	case %d:
						if rdt != '%c' { break }
						d := rdx.%cdelta(i.Value(), o.%s)
						delta = append(delta, chotki.EditTLV(%d, '%c', d))
						had = had | (1<<%d)
			`
var fragx = `       }
            	}
				for off:=uint64(1); off<%d; off++ {
					if (had&(1<<off))==0 {
			`
var fragr = "			delta = append(delta, chotki.EditTLV(off, '%c', rdx.%ctlv(o.%s)))\n"
var fragz = `   	}
				}
				return delta, nil
			}
			`

func (cho *Chotki) CompileClass(name string, id rdx.ID) (gosrc string, err error) {
	var decl []string
	var load []string
	var store []string
	var init []string
	var diff []string
	var fields []Field
	fields, err = cho.ClassFields(id)
	if err != nil {
		return
	}
	for off := 1; off < len(fields); off++ {
		field := &fields[off]
		switch field.RdxType {
		case rdx.Float, rdx.Integer, rdx.Reference, rdx.String, rdx.Term:
			code := fmt.Sprintf(fragn, off, field.RdxType, field.Name, field.RdxType)
			load = append(load, code)
			fd := fmt.Sprintf(fragf, field.Name, FIRSTnatives[field.RdxType])
			decl = append(decl, fd)
			in := fmt.Sprintf(fragi, field.RdxType, field.Name)
			init = append(init, in)
			st := fmt.Sprintf(fragr, field.RdxType, field.RdxType, field.Name)
			store = append(store, st)
			df := fmt.Sprintf(fraga, off, field.RdxType, field.RdxType, field.Name, off, field.RdxType, off)
			diff = append(diff, df)
		case rdx.Eulerian:
		case rdx.Linear:
		case rdx.Mapping:
		}
	}
	ret := []byte{}
	ret = append(ret, fmt.Sprintf(fragt, name)...)
	for _, d := range decl {
		ret = append(ret, d...)
	}
	ret = append(ret, fmt.Sprintf(frage, name, uint64(id))...)

	// Load()
	ret = append(ret, fmt.Sprintf(fragl, name, name)...)
	for _, l := range load {
		ret = append(ret, l...)
	}
	ret = append(ret, fragu...)

	// Store()
	ret = append(ret, fmt.Sprintf(frags, name)...)
	for _, s := range init {
		ret = append(ret, s...)
	}
	ret = append(ret, fmt.Sprintf(fragd, name)...)
	for _, d := range diff {
		ret = append(ret, d...)
	}
	ret = append(ret, fmt.Sprintf(fragx, len(fields))...)
	for _, s := range store {
		ret = append(ret, s...)
	}
	ret = append(ret, fragz...)

	return string(ret), nil
}
