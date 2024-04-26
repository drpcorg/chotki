package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
	"github.com/drpcorg/chotki/toytlv"
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
	} else if arg.RdxType == rdx.Mapping {
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

	dirname := chotki.ReplicaDirName(src.Src())
	repl.Host, err = chotki.Open(dirname, chotki.Options{
		Src:     src.Src(),
		Name:    name,
		Options: pebble.Options{ErrorIfExists: true},
	})
	if err == nil {
		id = repl.Host.Last()
	}
	return
}

var HelpOpen = errors.New("open zone/1")

func (repl *REPL) CommandOpen(arg *rdx.RDX) (rdx.ID, error) {
	if arg == nil || arg.RdxType != rdx.Reference {
		return rdx.BadId, HelpOpen
	}

	src0 := rdx.IDFromText(arg.Text)
	dirname := chotki.ReplicaDirName(src0.Src())

	var err error
	repl.Host, err = chotki.Open(dirname, chotki.Options{
		Src:     src0.Src(),
		Options: pebble.Options{ErrorIfNotExists: true},
	})
	if err != nil {
		return rdx.BadId, err
	}

	return repl.Host.Last(), nil
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
	if repl.snap != nil {
		_ = repl.snap.Close()
		repl.snap = nil
	}
	id = repl.Host.Last()
	err = repl.Host.Close()
	return
}

var HelpClass = errors.New(
	"class {_ref: 0-0, Name: S, Score: N}",
)

func (repl *REPL) CommandClass(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.BadId
	err = HelpClass
	if arg == nil || arg.RdxType != rdx.Mapping || len(arg.Nested) < 2 {
		return
	}
	fields := arg.Nested
	for i := 0; i+1 < len(fields); i += 2 {
		key := fields[i]
		val := fields[i+1]
		if string(key.Text) == "_ref" {
			continue
		}
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
	tlvs := utils.Records{}
	if arg == nil {
		return
	} else if arg.RdxType == rdx.Linear {
		return
	} else if arg.RdxType == rdx.Mapping {
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
		tmp := make(utils.Records, len(fields))

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
	if arg == nil || arg.RdxType != rdx.Mapping || len(arg.Nested) < 2 {
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
		err = repl.Host.Listen(context.Background(), addr)
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
		err = repl.Host.Connect(context.Background(), addr)
	}
	return
}

var HelpPing = errors.New("ping b0b-12-1 // S field id")

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

var HelpPinc = errors.New("pinc b0b-12-2")
var ErrBadField = errors.New("bad field")

func KeepOddEven(oddeven uint64, cho *chotki.Chotki, fid rdx.ID) error {
	rdt, tlv, err := cho.ObjectFieldTLV(fid)
	if err != nil || rdt != rdx.Natural {
		return ErrBadField
	}
	src := cho.Source()
	mine := rdx.Nmine(tlv, src)
	sum := rdx.Nnative(tlv)
	if (sum & 1) != oddeven {
		tlvs := utils.Records{
			toytlv.Record('F', rdx.ZipUint64(fid.Off())),
			toytlv.Record(rdx.Natural, toytlv.Record(rdx.Term, rdx.ZipUint64Pair(mine+1, src))),
		}
		_, err = cho.CommitPacket('E', fid.ZeroOff(), tlvs)
	}
	return err
}

func KeepOdd(cho *chotki.Chotki, fid rdx.ID) error {
	return KeepOddEven(1, cho, fid)
}

func KeepEven(cho *chotki.Chotki, fid rdx.ID) error {
	return KeepOddEven(0, cho, fid)
}

func (repl *REPL) CommandPinc(arg *rdx.RDX) (id rdx.ID, err error) {
	id, err = rdx.BadId, HelpPinc
	if arg == nil || arg.RdxType != rdx.Reference {
		return
	}
	fid := rdx.IDFromText(arg.Text)
	if fid.Off() == 0 {
		return
	}
	err = KeepOdd(repl.Host, fid)
	if err != nil {
		return
	}
	repl.Host.AddHook(fid, KeepOdd)
	id = fid
	err = nil
	return
}

func (repl *REPL) CommandPonc(arg *rdx.RDX) (id rdx.ID, err error) {
	id, err = rdx.BadId, HelpPinc
	if arg == nil || arg.RdxType != rdx.Reference {
		return
	}
	fid := rdx.IDFromText(arg.Text)
	if fid.Off() == 0 {
		return
	}
	err = KeepEven(repl.Host, fid)
	if err != nil {
		return
	}
	repl.Host.AddHook(fid, KeepEven)
	id = fid
	err = nil
	return
}

func (repl *REPL) CommandMute(arg *rdx.RDX) (id rdx.ID, err error) {
	id, err = rdx.BadId, HelpPinc
	if arg == nil || arg.RdxType != rdx.Reference {
		return
	}
	fid := rdx.IDFromText(arg.Text)
	if fid.Off() == 0 {
		return
	}
	repl.Host.RemoveAllHooks(fid)
	id = rdx.ID0
	err = nil
	return
}

var HelpTinc = errors.New("tinc b0b-12-2, tinc {fid:b0b-12-2,ms:1000,count:100}")

func (repl *REPL) doTinc(fid rdx.ID, delay time.Duration, count int64) {
	var err error
	for ; count > 0 && err == nil; count-- {
		_, err = repl.Host.IncNField(fid)
		if delay > time.Duration(0) {
			time.Sleep(delay)
		}
	}
}

// testing: read-inc loop
func (repl *REPL) CommandTinc(arg *rdx.RDX) (id rdx.ID, err error) {
	id, err = rdx.BadId, HelpTinc
	count := int64(1)
	delay := time.Second
	if arg == nil {
		return
	} else if arg.RdxType == rdx.Reference {
		id = rdx.IDFromText(arg.Text)
	} else if arg.RdxType == rdx.Mapping {
		for i := 0; i+1 < len(arg.Nested); i += 2 {
			key := &arg.Nested[i]
			val := &arg.Nested[i+1]
			switch key.String() {
			case "fid":
				id = rdx.IDFromText(val.Text)
			case "ms":
				ms := rdx.Inative(rdx.Iparse(val.String()))
				delay = time.Millisecond * time.Duration(ms)
			case "count":
				count = rdx.Inative(rdx.Iparse(val.String()))
			default:
				return
			}
		}
	} else {
		return
	}
	err = nil
	go repl.doTinc(id, delay, count)
	return
}

var HelpSinc = errors.New("sinc b0b-12-2, tinc {fid:b0b-12-2,ms:1000,count:100}")

func (repl *REPL) doSinc(fid rdx.ID, delay time.Duration, count int64, mine uint64) {
	var err error
	start := time.Now()
	fro := repl.Host.Last()
	src := fro.Src()
	til := rdx.ID0
	for c := count; c > 0 && err == nil; c-- {
		mine++
		tlvs := utils.Records{
			toytlv.Record('F', rdx.ZipUint64(fid.Off())),
			toytlv.Record(rdx.Natural, toytlv.Record(rdx.Term, rdx.ZipUint64Pair(mine, src))),
		}
		til, err = repl.Host.CommitPacket('E', fid.ZeroOff(), tlvs)
		if delay > time.Duration(0) {
			time.Sleep(delay)
		}
	}
	if err != nil {
		fmt.Println(err.Error())
	}
	timer := time.Since(start)
	_, _ = fmt.Fprintf(os.Stdout, "inc storm: %d incs complete for %s, elapsed %s, %s..%s\n",
		count, fid.String(), timer.String(), fro.String(), til.String())
}

func (repl *REPL) CommandSinc(arg *rdx.RDX) (id rdx.ID, err error) {
	id, err = rdx.BadId, HelpSinc
	count := int64(1)
	delay := time.Second
	if arg == nil {
		return
	} else if arg.RdxType == rdx.Reference {
		id = rdx.IDFromText(arg.Text)
	} else if arg.RdxType == rdx.Mapping {
		for i := 0; i+1 < len(arg.Nested); i += 2 {
			key := &arg.Nested[i]
			val := &arg.Nested[i+1]
			switch key.String() {
			case "fid":
				id = rdx.IDFromText(val.Text)
				if id.Off() == 0 {
					return rdx.BadId, chotki.ErrWrongFieldType
				}
			case "ms":
				ms := rdx.Inative(rdx.Iparse(val.String()))
				delay = time.Millisecond * time.Duration(ms)
			case "count":
				count = rdx.Inative(rdx.Iparse(val.String()))
			default:
				return
			}
		}
	} else {
		return
	}

	rdt, tlv, err := repl.Host.ObjectFieldTLV(id)
	if err != nil || rdt != rdx.Natural {
		return rdx.BadId, chotki.ErrWrongFieldType
	}
	src := repl.Host.Source()
	mine := rdx.Nmine(tlv, src)

	go repl.doSinc(id, delay, count, mine)
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
		repl.Host.AddHook(id, func(cho *chotki.Chotki, id rdx.ID) error {
			fmt.Println("field changed")
			return nil
		})
		err = nil
	} else {
		return
	}
	return
}

var HelpName = errors.New("name, name Obj, name {Obj: b0b-12-1}")

func (repl *REPL) CommandName(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.BadId
	var names rdx.MapTR
	names, err = repl.Host.ObjectFieldMapTermId(chotki.IdNames)
	if err != nil {
		return
	}
	if arg == nil || arg.RdxType == rdx.None {
		fmt.Println(names.String())
		id = repl.Host.Last()
	} else if arg.RdxType == rdx.Term {
		key := string(arg.Text)
		fmt.Printf("{%s:%s}\n", key, names[key])
	} else if arg.RdxType == rdx.Mapping {
		_, tlv, _ := repl.Host.ObjectFieldTLV(chotki.IdNames)
		parsed := rdx.MparseTR(arg)
		delta := toytlv.Record('M', rdx.MdeltaTR(tlv, parsed))
		id, err = repl.Host.EditFieldTLV(chotki.IdNames, delta)
	} else {
		err = HelpName
	}
	return
}

func (repl *REPL) CommandValid(arg *rdx.RDX) (id rdx.ID, err error) {
	it := repl.Host.Database().NewIter(&pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	})
	key := chotki.OKey(rdx.ID0, 'A')
	for it.SeekGE(key); it.Valid(); it.Next() {
		id, rdt := chotki.OKeyIdRdt(it.Key())
		val, e := it.ValueAndErr()
		if e != nil {
			_, _ = fmt.Fprintf(os.Stderr, "record read fail %s\n", e.Error())
			continue
		}
		if !rdx.Xvalid(rdt, val) {
			fmt.Printf("%c record is not valid at %s\n", rdt, id.String())
		}
	}
	_ = it.Close()
	return rdx.ID0, nil
}

var HelpCompile = errors.New("choc ClassName, choc b0b-2, choc {ClassName:b0b-2}")

func (repl *REPL) CommandCompile(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.ID0
	var code string
	orm := repl.Host.ObjectMapper()
	if arg == nil {
		return rdx.BadId, HelpCompile
	} else if arg.RdxType == rdx.Reference {
		id = rdx.IDFromText(arg.Text)
		code, err = orm.Compile("SomeClass", id)
		//repl.Host.CompileClass("SomeClass", id)
	} else if arg.RdxType == rdx.Mapping {
		m := arg.Nested
		for i := 0; i+1 < len(m) && err == nil; i += 2 {
			name := &m[i]
			cidx := &m[i+1]
			if name.RdxType != rdx.Term || cidx.RdxType != rdx.Reference {
				return rdx.BadId, HelpCompile
			}
			cid := rdx.IDFromText(cidx.Text)
			c := ""
			c, err = orm.Compile(string(name.Text), cid)
			code = code + c
		}
	} else {
		return
	}
	if err == nil {
		fmt.Println(code)
	}
	return
}
