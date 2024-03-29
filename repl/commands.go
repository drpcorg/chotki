package main

import (
	"errors"
	"fmt"
	"github.com/drpcorg/chotki/rdx"
)

var HelpCreate = errors.New("create zone/1 {Name:\"Name\",Description:\"long text\"}")

func (repl *REPL) CommandCreate(path *rdx.RDX, arg *rdx.RDX) (err error) {
	if path == nil || len(path.Nested) == 0 {
		return HelpCreate
	}
	var src uint64
	last := path.Nested[len(path.Nested)-1]
	if last.RdxType != rdx.RdxInt {
		return HelpCreate
	}
	_, _ = fmt.Sscanf(string(last.String()), "%d", &src)
	name := "name TODO"
	err = repl.Host.Create(src, name)
	if err == nil {
		fmt.Printf("replica %d created\n", src)
	}
	return
}

var HelpOpen = errors.New("open zone/1")

func (repl *REPL) CommandOpen(path *rdx.RDX, arg *rdx.RDX) (err error) {
	if path == nil || len(path.Nested) == 0 {
		return HelpOpen
	}
	var src uint64
	last := path.Nested[len(path.Nested)-1]
	if last.RdxType != rdx.RdxInt {
		return HelpOpen
	}
	_, _ = fmt.Sscanf(string(last.String()), "%d", &src)
	err = repl.Host.Open(src)
	if err == nil {
		fmt.Printf("replica %d opened\n", src)
	}
	return
}

func (repl *REPL) CommandClose(path *rdx.RDX, arg *rdx.RDX) (err error) {
	err = repl.Host.Close()
	if err == nil {
		fmt.Printf("replica closed\n")
	}
	return
}

func (repl *REPL) CommandNew(path *rdx.RDX, arg *rdx.RDX) error {
	fmt.Printf("new %s %s\n", path.String(), arg.String())
	return nil
}
