package main

import (
	"fmt"
	"github.com/drpcorg/chotki/rdx"
)

func (repl *REPL) CommandNew(path *rdx.RDX, arg *rdx.RDX) error {
	fmt.Printf("new %s %s\n", path.String(), arg.String())
	return nil
}
