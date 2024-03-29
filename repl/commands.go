package main

import (
	"errors"
	"fmt"
	"github.com/drpcorg/chotki/rdx"
)

var HelpCreate = errors.New("create zone/1 {Name:\"Name\",Description:\"long text\"}")

func (repl *REPL) CommandCreate(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	if path == nil || len(path.Nested) == 0 {
		return rdx.BadId, HelpCreate
	}
	var src uint64
	last := path.Nested[len(path.Nested)-1]
	if last.RdxType != rdx.RdxInt {
		return rdx.BadId, HelpCreate
	}
	_, _ = fmt.Sscanf(string(last.String()), "%d", &src)
	name := "name TODO"
	err = repl.Host.Create(src, name)
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
	var src uint64
	last := path.Nested[len(path.Nested)-1]
	if last.RdxType != rdx.RdxInt {
		return rdx.BadId, HelpOpen
	}
	_, _ = fmt.Sscanf(string(last.String()), "%d", &src)
	err = repl.Host.Open(src)
	if err == nil {
		id = repl.Host.Last()
	}
	return
}

func (repl *REPL) CommandClose(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	err = repl.Host.Close()
	if err == nil {
		id = repl.Host.Last()
	}
	return
}

var HelpType = errors.New("type Parent [\"SName\", \"IAge\"]")

func (repl *REPL) CommandType(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	if path == nil || len(path.Nested) == 0 || arg == nil || len(arg.Nested) == 0 {
		return rdx.BadId, HelpType
	}
	var fields []string
	for _, f := range arg.Nested {
		if f.RdxType != rdx.RdxString {
			return rdx.BadId, HelpType
		}
		tlv := rdx.Sparse(f.String())
		form := rdx.Snative(tlv)
		if len(form) < 2 || form[0] < 'A' || form[0] > 'Z' {
			return rdx.BadId, HelpType
		}
		fields = append(fields, form)
	}
	id, err = repl.Host.CreateType(rdx.ID0, fields...)
	return
}

func (repl *REPL) CommandNew(path *rdx.RDX, arg *rdx.RDX) (id rdx.ID, err error) {
	fmt.Printf("new %s %s\n", path.String(), arg.String())
	return rdx.BadId, nil
}
