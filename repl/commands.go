package main

import (
	"errors"
	"fmt"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"net"
)

var HelpCreate = errors.New("create zone/1 {Name:\"Name\",Description:\"long text\"}")

func (repl *REPL) CommandCreate(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	if path == nil || len(path.Nested) == 0 {
		return rdx.BadId, HelpCreate
	}
	last := path.Nested[len(path.Nested)-1]
	if last.RdxType != rdx.Reference {
		return rdx.BadId, HelpCreate
	}
	src0 := rdx.IDFromText(last.Text)
	name := "name TODO"
	err = repl.Host.Create(src0.Src(), name)
	if err == nil {
		id = repl.Host.Last()
	}
	return
}

var HelpOpen = errors.New("open zone/1")

func (repl *REPL) CommandOpen(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	if path == nil || len(path.Nested) == 0 {
		return rdx.BadId, HelpOpen
	}
	last := path.Nested[len(path.Nested)-1]
	if last.RdxType != rdx.Reference {
		return rdx.BadId, HelpOpen
	}
	src0 := rdx.IDFromText(last.Text)
	err = repl.Host.Open(src0.Src())
	if err == nil {
		id = repl.Host.Last()
	}
	return
}

var HelpDump = errors.New("dump (obj|objects|vv|all)?")

func (repl *REPL) CommandDump(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	if path != nil && path.RdxType == rdx.RdxPath && len(path.Nested) > 0 && path.Nested[0].RdxType == rdx.RdxName {
		name := path.Nested[0].String()
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

func (repl *REPL) CommandClose(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
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

var HelpType = errors.New("type Parent [\"SName\", \"IAge\"]")

func (repl *REPL) CommandClass(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	if path == nil || len(path.Nested) == 0 || arg == nil || len(arg.Nested) == 0 {
		return rdx.BadId, HelpType
	}
	var fields []string
	for _, f := range arg.Nested {
		if f.RdxType != rdx.String {
			return rdx.BadId, HelpType
		}
		tlv := rdx.Sparse(f.String())
		form := rdx.Snative(tlv)
		if len(form) < 2 || form[0] < 'A' || form[0] > 'Z' {
			return rdx.BadId, HelpType
		}
		fields = append(fields, form)
	}
	id, err = repl.Host.NewClass(rdx.ID0, fields...)
	return
}

var ErrBadArgs = errors.New("bad arguments")

func (repl *REPL) CommandNew(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	tid := rdx.ID0
	if path != nil && path.RdxType == rdx.RdxPath && len(path.Nested) > 0 && path.Nested[0].RdxType == rdx.Reference {
		tid = rdx.IDFromText(path.Nested[0].Text)
	}
	if arg == nil || arg.RdxType != rdx.LArray {
		return rdx.BadId, ErrBadArgs
	}
	fields := []string{}
	for _, a := range arg.Nested {
		fields = append(fields, string(a.Text))
	}
	id, err = repl.Host.NewObject(tid, fields...)
	return
}

// N counters:
// - tlv heap
// - merge oper
// - N* fns
// - N node (list: contribs)
// - try set 3-3.Score.3 5
// - fixme LOT rdt T (COLA)

func (repl *REPL) CommandList(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	node := repl.NodeByPath(path)
	id = node.ID()
	list := node.List()
	for _, key := range list {
		val := node.Get(key)
		str := ""
		if val != nil {
			str = val.String()
		}
		fmt.Printf("%s:\t%s\n", key, str)
	}
	return
}

func (repl *REPL) CommandCat(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	node := repl.NodeByPath(path)
	id = node.ID()
	val := node.String()
	fmt.Printf("%s:\t%s\n", id.String(), val)
	return
}

var HelpAddress = errors.New("address syntax: 11.22.33.44 1234, host.com, etc")

func (repl *REPL) ParseAddress(path *rdx.RDX, arg *rdx.RDX) (addr string, err error) {
	if repl.Host.Last() == rdx.ID0 {
		return "", chotki.ErrClosed
	}
	addr = "0.0.0.0"
	if path != nil {
		addr = string(path.Text)
	}
	if arg == nil {
		addr += ":1234"
	} else if arg.RdxType == rdx.Integer {
		addr += ":" + string(arg.Text)
	} else {
		return "", HelpAddress
	}
	if repl.tcp == nil {
		repl.tcp = &toytlv.TCPDepot{}
		repl.tcp.Open(func(conn net.Conn) toyqueue.FeedDrainCloser {
			return &chotki.Syncer{
				Host: &repl.Host,
				Mode: chotki.SyncRW,
				Name: conn.RemoteAddr().String(),
			}
		})
	}
	return
}

var HelpListen = errors.New("listen 11.22.33.44 1234")

func (repl *REPL) CommandListen(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	addr := ""
	addr, err = repl.ParseAddress(path, arg)
	if err == nil {
		err = repl.tcp.Listen(addr)
	}
	return
}

var HelpConnect = errors.New("connect 11.22.33.44 1234")

func (repl *REPL) CommandConnect(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	addr := ""
	addr, err = repl.ParseAddress(path, arg)
	if err == nil {
		err = repl.tcp.Connect(addr)
	}
	return
}
