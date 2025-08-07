package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

func replicaDirName(rno uint64) string {
	return fmt.Sprintf("cho%x", rno)
}

func (repl *REPL) idFromNameOrText(a *rdx.RDX) (id rdx.ID, err error) {
	switch a.RdxType {
	case rdx.Reference:
		id = rdx.IDFromText(a.Text)
	case rdx.Term:
		var names rdx.MapTR
		names, err = repl.Host.MapTRField(chotki.IdNames)
		if oid, ok := names[a.String()]; !ok {
			err = fmt.Errorf("No such name")
			return
		} else {
			id = oid
		}
	default:
		err = fmt.Errorf("Wrong type")
	}
	return
}

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

	dirname := replicaDirName(src.Src())
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
	dirname := replicaDirName(src0.Src())

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

func (repl *REPL) CommandOpenDir(arg *rdx.RDX) (rdx.ID, error) {
	if arg.RdxType != rdx.String {
		return rdx.BadId, fmt.Errorf("unable to open")
	}
	var err error
	fmt.Println(rdx.Snative(rdx.Sparse(string(arg.Text))))
	repl.Host, err = chotki.Open(rdx.Snative(rdx.Sparse(string(arg.Text))), chotki.Options{
		Src:     0xa,
		Options: pebble.Options{},
	})
	if err != nil {
		return rdx.BadId, err
	}

	return repl.Host.Last(), nil
}

var HelpCheckpoint = errors.New("cp \"monday\"")

func (repl *REPL) CommandCheckpoint(arg *rdx.RDX) (rdx.ID, error) {
	if arg == nil || arg.RdxType != rdx.String {
		return rdx.BadId, HelpCheckpoint
	}
	tlv := rdx.Sparse(string(arg.Text))
	name := rdx.Snative(tlv)
	parent := repl.Host.Directory()
	path := filepath.Join(parent, name)
	err := repl.Host.Database().Checkpoint(path)
	return rdx.ID0, err
}

var HelpDump = errors.New("dump (obj|objects|vv|all)?")

func (repl *REPL) CommandDump(arg *rdx.RDX) (id rdx.ID, err error) {
	if arg != nil && arg.RdxType == rdx.Term {
		name := string(arg.Text)
		switch name {
		case "obj", "objects":
			repl.Host.DumpObjects(os.Stderr)
		case "vv":
			repl.Host.DumpVV(os.Stderr)
		case "all":
			file, err := os.OpenFile("test.log", os.O_RDWR|os.O_CREATE, 0660)
			if err != nil {
				panic(err)
			}
			repl.Host.DumpAll(file)
			file.Close()
		default:
			return rdx.BadId, HelpDump
		}
	} else {
		repl.Host.DumpAll(os.Stderr)
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
	if arg == nil {
		return
	} else if arg.RdxType == rdx.Mapping {
		fields := arg.Nested
		parent := rdx.ID0
		decl := protocol.Records{}
		n := int64(1)
		for i := 0; i+1 < len(fields); i += 2 {
			key := fields[i]
			val := fields[i+1]
			if string(key.Text) == "_ref" {
				if val.RdxType != rdx.Reference && val.RdxType != rdx.Term || parent != rdx.ID0 {
					return
				}
				parent, err = repl.idFromNameOrText(&val)
				continue
			}
			if key.RdxType != rdx.Term || val.RdxType != rdx.Term {
				return
			}
			if len(val.Text) != 1 || val.Text[0] > 'Z' || val.Text[0] < 'A' {
				return // todo support typed containers, e.g. MSS
			}
			desc := append([]byte{}, val.Text[0])
			desc = append(desc, key.Text...)
			tok := rdx.FIRSTtlv(n, 0, desc)
			decl = append(decl, protocol.Record('T', tok))
			n++
		}
		id, err = repl.Host.CommitPacket(context.Background(), 'C', parent, decl)
	} else if arg.RdxType == rdx.Reference {
		cid := rdx.IDFromText(arg.Text)
		tlv, err := repl.Host.GetClassTLV(context.Background(), cid)
		if err != nil {
			return rdx.BadId, err
		}
		fmt.Println(rdx.Xstring('C', tlv))
		return cid, nil
	}
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
	tlvs := protocol.Records{}
	if arg == nil {
		return
	} else if arg.RdxType == rdx.Linear {
		return
	} else if arg.RdxType == rdx.Term {
		// todo default object
		return
	} else if arg.RdxType == rdx.Mapping {
		pairs := arg.Nested
		if len(pairs) >= 2 && pairs[0].String() == "_ref" {
			if pairs[1].RdxType != rdx.Reference && pairs[1].RdxType != rdx.Term {
				return
			}
			tid, err = repl.idFromNameOrText(&pairs[1])
			if err != nil {
				return id, err
			}
			pairs = pairs[2:]
		}
		var fields chotki.Fields
		fields, err = repl.Host.ClassFields(tid)
		if err != nil {
			return
		}
		tmp := make(protocol.Records, len(fields))

		for i := 0; i+1 < len(pairs); i += 2 {
			if pairs[i].RdxType != rdx.Term {
				return
			}
			name := pairs[i].String()
			value := &pairs[i+1]
			ndx := fields.FindName(name) //fixme rdt
			if ndx == -1 {
				err = fmt.Errorf("unknown field %s\n", name)
				return
			}
			fieldType := fields[ndx].RdxType
			if value.RdxType != fieldType {
				if value.RdxType == rdx.Integer && (fieldType == rdx.Natural || fieldType == rdx.ZCounter) {
					value.RdxType = fieldType
				} else {
					err = fmt.Errorf("wrong type for %s\n", name)
				}
			}
			tmp[ndx] = rdx.FIRSTrdx2tlv(value)
		}
		for i := 1; i < len(fields); i++ {
			rdt := fields[i].RdxType
			if tmp[i] == nil {
				tlvs = append(tlvs, protocol.Record(rdt, rdx.Xdefault(rdt)))
			} else {
				tlvs = append(tlvs, protocol.Record(rdt, tmp[i]))
			}
		}
	} else {
		return
	}
	id, err = repl.Host.CommitPacket(context.Background(), 'O', tid, tlvs)
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
		if arg.Nested[1].RdxType != rdx.Reference && arg.Nested[1].RdxType != rdx.Term {
			return
		}
		var oid rdx.ID
		oid, err = repl.idFromNameOrText(&arg.Nested[1])
		if err != nil {
			return id, err
		}
		return repl.Host.EditObjectRDX(context.Background(), oid, arg.Nested[2:])
	} else { // todo
		return
	}
}

var HelpAdd = errors.New(
	"add {b0b-1e-2: +3, a1ece-3f0-2: +7}",
)

func (repl *REPL) CommandAdd(arg *rdx.RDX) (id rdx.ID, err error) {
	if arg.RdxType == rdx.Mapping {
		pairs := arg.Nested
		for i := 0; i+1 < len(pairs) && err == nil; i += 2 {
			if pairs[i].RdxType != rdx.Reference && pairs[i].RdxType != rdx.Term || pairs[i+1].RdxType != rdx.Integer {
				return rdx.BadId, HelpAdd
			}
			var fid rdx.ID
			fid, err = repl.idFromNameOrText(&pairs[i])
			if err != nil {
				return id, err
			}
			var add uint64
			_, err = fmt.Sscanf(string(pairs[i+1].Text), "%d", &add)
			if fid.Off() == 0 || err != nil {
				return
			}
			id, err = repl.Host.AddToNField(context.Background(), fid, add)
		}

	} else {
		return rdx.BadId, HelpAdd
	}
	return
}

var HelpInc = errors.New(
	"inc b0b-1e-2",
)

func (repl *REPL) CommandInc(arg *rdx.RDX) (id rdx.ID, err error) {
	id = rdx.BadId
	err = HelpInc
	if arg.RdxType == rdx.Reference || arg.RdxType == rdx.Term {
		fid := rdx.IDFromText(arg.Text)
		if id.Off() == 0 {
			return
		}
		id, err = repl.Host.IncNField(context.Background(), fid)
	}
	return
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
	if arg == nil || arg.RdxType != rdx.Reference && arg.RdxType != rdx.Term {
		return
	}
	var oid rdx.ID
	oid, err = repl.idFromNameOrText(arg)
	if err != nil {
		return id, err
	}
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
	err = repl.Host.Listen(addr)
	return
}

var HelpConnect = errors.New("connect \"11.22.33.44:1234\"")

func (repl *REPL) CommandConnect(arg *rdx.RDX) (id rdx.ID, err error) {
	if arg == nil || arg.RdxType != rdx.String {
		return rdx.BadId, HelpConnect
	}
	addr := rdx.Snative(rdx.Sparse(string(arg.Text)))
	err = repl.Host.Connect(addr)
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
	names, err = repl.Host.MapTRField(chotki.IdNames)
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
		delta := protocol.Record('M', rdx.MdeltaTR(tlv, parsed, repl.Host.Clock()))
		id, err = repl.Host.EditFieldTLV(context.Background(), chotki.IdNames, delta)
	} else {
		err = HelpName
	}
	return
}

func (repl *REPL) CommandSwagger(arg *rdx.RDX) (id rdx.ID, err error) {
	mux := http.NewServeMux()
	fs := http.FileServer(http.Dir("./swagger"))

	mux.Handle("/", fs)
	mux.HandleFunc("/swagger.yaml", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./swagger/swagger.yaml")
	})

	go func() {
		err := http.ListenAndServe("127.0.0.1:8000", mux)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to serve: %s\n", err.Error())
		}
	}()

	return
}

var HelpServeHttp = errors.New("servehttp 8001")

func (repl *REPL) CommandServeHttp(arg *rdx.RDX) (id rdx.ID, err error) {
	if arg == nil || arg.RdxType != rdx.Integer {
		return rdx.BadId, HelpServeHttp
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/listen", AddCorsHeaders(ListenHandler(repl)))
	mux.HandleFunc("/connect", AddCorsHeaders(ConnectHandler(repl)))
	mux.HandleFunc("/class", AddCorsHeaders(ClassHandler(repl)))
	mux.HandleFunc("/name", AddCorsHeaders(NameHandler(repl)))
	mux.HandleFunc("/new", AddCorsHeaders(NewHandler(repl)))
	mux.HandleFunc("/edit", AddCorsHeaders(EditHandler(repl)))
	mux.HandleFunc("/cat", AddCorsHeaders(CatHandler(repl)))
	mux.HandleFunc("/list", AddCorsHeaders(ListHandler(repl)))

	go func() {
		err := http.ListenAndServe("127.0.0.1:"+arg.String(), mux)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to serve: %s\n", err.Error())
		}
	}()

	return
}
