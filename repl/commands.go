package main

import (
	"errors"
	"fmt"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
)

var HelpCreate = errors.New("create zone/1 {Name:\"Name\",Description:\"long text\"}")

func (repl *REPL) CommandCreate(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.BadId
	err = HelpCreate
	if arg == nil {
		return
	}
	src := rdx.ID0
	name := "Unnamed replica"
	if arg.RdxType == rdx.Reference {
		src = rdx.IDFromText(arg.Text)
	} else if arg.RdxType == rdx.Map {
		for i := 0; i+1 < len(arg.Nested); i += 2 {
			key := arg.Nested[i]
			val := arg.Nested[i+1]
			if key.RdxType != rdx.Term {
				return
			}
			term := string(key.Text)
			value := string(val.Text)
			switch term {
			case "Name":
				if val.RdxType != rdx.String {
					return
				}
				name = rdx.Snative(rdx.Sparse(term))
			case "_id":
				if val.RdxType != rdx.Reference {
					return
				}
				src = rdx.Rnative(rdx.Rparse(value))
			default:
				return
			}
		}
	}
	if src == rdx.ID0 {
		return
	}
	err = repl.Host.Create(src.Src(), name)
	if err == nil {
		id = repl.Host.Last()
	}
	return
}

var HelpOpen = errors.New("open zone/1")

func (repl *REPL) CommandOpen(arg *rdx.RDX) (id rdx.ID, err error) {
	if arg == nil || arg.RdxType != rdx.Reference {
		return rdx.BadId, HelpOpen
	}
	src0 := rdx.IDFromText(arg.Text)
	err = repl.Host.Open(src0.Src())
	if err == nil {
		id = repl.Host.Last()
	}
	return
}

var HelpDump = errors.New("dump (obj|objects|vv|all)?")

func (repl *REPL) CommandDump(arg *rdx.RDX) (id rdx.ID, err error) {
	if arg != nil && arg.RdxType == rdx.Term {
		name := string(arg.Text)
		switch name {
		case "obj", "objects":
			repl.Host.DumpObjects()
		case "vv":
			repl.Host.DumpVV()
		case "all":
			repl.Host.DumpAll()
		default:
			return rdx.BadId, HelpDump
		}
	} else {
		repl.Host.DumpAll()
	}
	return
}

func (repl *REPL) CommandClose(arg *rdx.RDX) (id rdx.ID, err error) {
	if repl.tcp != nil {
		repl.tcp.Close()
		repl.tcp = nil
	}
	err = repl.Host.Close()
	if err == nil {
		id = repl.Host.Last()
	}
	return
}

var HelpClass = errors.New(
	"class {_ref: 0-0, Name: S, Score: N}",
)

func (repl *REPL) CommandClass(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.BadId
	err = HelpClass
	if arg == nil || arg.RdxType != rdx.Map || len(arg.Nested) < 2 {
		return
	}
	fields := arg.Nested
	for i := 0; i+1 < len(fields); i += 2 {
		key := fields[i]
		val := fields[i+1]
		if key.RdxType != rdx.Term || val.RdxType != rdx.Term {
			return
		}
		// TODO more checks
	}
	parent := rdx.ID0
	if string(fields[0].Text) == "_ref" {
		if fields[1].RdxType != rdx.Reference {
			return
		}
		parent = rdx.IDFromText(fields[1].Text)
		fields = fields[2:]
	}
	tlvs := rdx.FIRSTrdxs2tlvs(fields)
	id, err = repl.Host.CommitPacket('C', parent, tlvs)
	return
}

var ErrBadArgs = errors.New("bad arguments")

var HelpNew = errors.New(
	"new {_ref: Student, Name: \"Ivan Petrov\", Score: 118}, " +
		"new [Student, \"Ivan Petrov\", 118]",
)

func (repl *REPL) CommandNew(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.BadId
	err = HelpNew
	tid := rdx.ID0
	tlvs := toyqueue.Records{}
	if arg == nil {
		return
	} else if arg.RdxType == rdx.LArray {
		return
	} else if arg.RdxType == rdx.Map {
		pairs := arg.Nested
		if len(pairs) >= 2 && pairs[0].String() == "_ref" {
			tid = rdx.IDFromText(pairs[1].Text)
			pairs = pairs[2:]
		}
		var fields chotki.Fields
		fields, err = repl.Host.ClassFields(tid)
		if err != nil {
			return
		}
		tmp := make(toyqueue.Records, len(fields))

		for i := 0; i+1 < len(pairs); i += 2 {
			if pairs[i].RdxType != rdx.Term {
				return
			}
			name := pairs[i].String()
			value := &pairs[i+1]
			ndx := fields.Find(name) //fixme rdt
			if ndx == -1 {
				err = fmt.Errorf("unknown field %s", name)
				return
			}
			if value.RdxType != fields[ndx].RdxType {
				err = fmt.Errorf("wrong type for %s", name)
			}
			tmp[ndx] = rdx.FIRSTrdx2tlv(value)
		}
		for i := 1; i < len(fields); i++ {
			rdt := fields[i].RdxType
			if tmp[i] == nil {
				tlvs = append(tlvs, toytlv.Record(rdt, rdx.Xdefault(rdt)))
			} else {
				tlvs = append(tlvs, toytlv.Record(rdt, tmp[i]))
			}
		}
	} else {
		return
	}
	id, err = repl.Host.CommitPacket('O', tid, tlvs)
	return
}

var HelpEdit = errors.New(
	"edit {_id: b0b-1e, Score: [+1], Pass: true}",
)

func (repl *REPL) CommandEdit(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.BadId
	err = HelpEdit
	if arg == nil || arg.RdxType != rdx.Map || len(arg.Nested) < 2 {
		return
	}
	if arg.Nested[0].String() == "_id" {
		if arg.Nested[1].RdxType != rdx.Reference {
			return
		}
		oid := rdx.IDFromText(arg.Nested[1].Text)
		return repl.Host.EditObjectRDX(oid, arg.Nested[2:])
	} else { // todo
		return
	}
}

var HelpList = errors.New(
	"ls b0b-1e",
)

func (repl *REPL) ListObject(oid rdx.ID) (txt []string, err error) {
	_, form, fact, e := repl.Host.ObjectFields(oid)
	if e != nil {
		return nil, e
	}
	txt = append(txt, "{")
	for n, d := range form {
		if n != 0 {
			txt = append(txt, ",")
		}
		switch d.RdxType {
		case 'F', 'I', 'S', 'T':
			value := rdx.Xstring(byte(d.RdxType), fact[n])
			txt = append(txt, fmt.Sprintf("%s:\t%s\n", d.Name, value))
		case 'R':
			recid := rdx.Rnative(fact[n])
			rectxt, rec := repl.ListObject(recid)
			if rec != nil {
				value := rdx.Xstring(byte(d.RdxType), fact[n])
				txt = append(txt, fmt.Sprintf("%s:\t%s\n", d.Name, value))
			} else {
				txt = append(txt, rectxt...)
			}
		default:
			txt = append(txt, "TODO")
		}
	}
	txt = append(txt, "}")
	return
}

func (repl *REPL) CommandList(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.BadId
	err = HelpCat
	if arg == nil || arg.RdxType != rdx.Reference {
		return
	}
	id = rdx.IDFromText(arg.Text)
	var strs []string
	strs, err = repl.ListObject(id)
	if err == nil {
		for _, s := range strs {
			fmt.Print(s)
		}
		fmt.Println()
	}
	return
}

var HelpCat = errors.New(
	"cat b0b-1e",
)

func (repl *REPL) CommandCat(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.BadId
	err = HelpCat
	if arg == nil || arg.RdxType != rdx.Reference {
		return
	}
	oid := rdx.IDFromText(arg.Text)
	var txt string
	txt, err = repl.Host.ObjectString(oid)
	if err != nil {
		return
	}
	fmt.Println(txt)
	err = nil
	id = oid
	return
}

var HelpListen = errors.New("listen \"11.22.33.44:1234\"")

func (repl *REPL) CommandListen(arg *rdx.RDX) (id rdx.ID, err error) {
	if arg == nil || arg.RdxType != rdx.String {
		return rdx.BadId, HelpListen
	}
	addr := rdx.Snative(rdx.Sparse(string(arg.Text)))
	if err == nil {
		if repl.tcp == nil {
			repl.tcp = &toytlv.TCPDepot{}
			repl.Host.OpenTCP(repl.tcp)
		}
		err = repl.tcp.Listen(addr)
	}
	return
}

var HelpConnect = errors.New("connect \"11.22.33.44:1234\"")

func (repl *REPL) CommandConnect(arg *rdx.RDX) (id rdx.ID, err error) {
	if arg == nil || arg.RdxType != rdx.String {
		return rdx.BadId, HelpConnect
	}
	addr := rdx.Snative(rdx.Sparse(string(arg.Text)))
	if err == nil {
		if repl.tcp == nil {
			repl.tcp = &toytlv.TCPDepot{}
			repl.Host.OpenTCP(repl.tcp)
		}
		err = repl.tcp.Connect(addr)
	}
	return
}

var HelpPing = errors.New("ping b0b-12-1")

func (repl *REPL) CommandPing(arg *rdx.RDX) (id rdx.ID, err error) {
	if arg == nil || arg.RdxType != rdx.Reference {
		return rdx.BadId, HelpPing
	}
	fid := rdx.IDFromText(arg.Text)
	oid := fid.ZeroOff()
	_, form, fact, e := repl.Host.ObjectFields(oid)
	if e != nil {
		return rdx.BadId, e
	}
	off := fid.Off()
	if off == 0 || int(off) > len(form) {
		return rdx.BadId, HelpPing
	}
	if form[off].RdxType != rdx.String {
		return rdx.BadId, errors.New(form[off].Name + " is not a string")
	}
	fmt.Printf("pinging through %s (field %s, previously %s)\n",
		fid.String(), form[off].Name, rdx.Snative(fact[off]))
	id, err = repl.Host.SetFieldTLV(fid, toytlv.Record('S', rdx.Stlv("ping")))
	return
}

func (repl *REPL) CommandTick(arg *rdx.RDX) (id rdx.ID, err error) {
	return
}

var HelpTell = errors.New("tell b0b-12-1")

func (repl *REPL) CommandTell(arg *rdx.RDX) (id rdx.ID, err error) {
	err = HelpTell
	id = rdx.BadId
	if arg == nil {
		return
	} else if arg.RdxType == rdx.Reference {
		id = rdx.IDFromText(arg.Text)
		repl.Host.AddHook(id, func(id rdx.ID) error {
			fmt.Println("field changed")
			return nil
		})
		err = nil
	} else {
		return
	}
	return
}
