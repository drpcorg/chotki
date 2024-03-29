package main

import (
	"errors"
	"fmt"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/ergochat/readline"
	"io"
	"os"
	"strings"
)

type Node interface {
	ID() chotki.ID
	String() string
	List() []string
	Get(name string) Node
	Put(loc string, node Node) error
	Set(val string) error
}

var ErrNotSupported = errors.New("operation not supported")

type AliasNode struct {
	Names map[string]chotki.ID
}

func (a *AliasNode) ID() chotki.ID {
	return chotki.ID0
}
func (a *AliasNode) String() string {
	return "-ALIASES-"
}
func (a *AliasNode) List() (ret []string) {
	for name, _ := range a.Names {
		ret = append(ret, name)
	}
	return
}
func (a *AliasNode) Get(name string) Node {
	id, ok := a.Names[name]
	if !ok {
		return nil
	} else if id.Off() != 0 {
		// see type
		return nil // &FieldNode{Id: id}
	} else {
		return &ObjectNode{Id: id}
	}

}
func (a *AliasNode) Put(loc string, node Node) error {
	a.Names[loc] = node.ID()
	return nil
}
func (a *AliasNode) Set(val string) error {
	return ErrNotSupported
}

type ObjectNode struct {
	Id   chotki.ID
	Host *chotki.Chotki
}

func (on *ObjectNode) ID() chotki.ID {
	return on.Id
}
func (on *ObjectNode) String() string {
	// todo find type, scan
	return "-obj-"
}
func (on *ObjectNode) List() []string {
	// todo find type, scan
	return nil
}
func (on *ObjectNode) Get(name string) Node {
	// todo find f in type decl
	return nil
}
func (on *ObjectNode) Put(loc string, node Node) error {
	// todo find f in type decl
	return nil
}
func (on *ObjectNode) Set(val string) error {
	return chotki.ErrNotImplemented // {key="value"}
}

type FNode struct {
	Id   chotki.ID
	Host *chotki.Chotki
}

func (fn *FNode) findType() {
	// db
}

func (fn *FNode) ID() chotki.ID {
	return fn.Id
}
func (fn *FNode) String() string {
	// get
	return ""
}
func (fn *FNode) List() []string {
	return []string{}
}
func (fn *FNode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *FNode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *FNode) Set(val string) error {
	return chotki.ErrNotImplemented
}

type INode struct {
	Id   chotki.ID
	Host *chotki.Chotki
}

func (fn *INode) findType() {
	// db
}

func (fn *INode) ID() chotki.ID {
	return fn.Id
}
func (fn *INode) String() string {
	// get
	return ""
}
func (fn *INode) List() []string {
	return []string{}
}
func (fn *INode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *INode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *INode) Set(val string) error {
	return chotki.ErrNotImplemented
}

type RNode struct {
	Id   chotki.ID
	Host *chotki.Chotki
}

func (fn *RNode) findType() {
	// db
}

func (fn *RNode) ID() chotki.ID {
	return fn.Id
}
func (fn *RNode) String() string {
	// get
	return ""
}
func (fn *RNode) List() []string {
	return []string{}
}
func (fn *RNode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *RNode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *RNode) Set(val string) error {
	return chotki.ErrNotImplemented
}

type SNode struct {
	Id   chotki.ID
	Host *chotki.Chotki
}

func (fn *SNode) findType() {
	// db
}

func (fn *SNode) ID() chotki.ID {
	return fn.Id
}
func (fn *SNode) String() string {
	// get
	return ""
}
func (fn *SNode) List() []string {
	return []string{}
}
func (fn *SNode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *SNode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *SNode) Set(val string) error {
	return chotki.ErrNotImplemented
}

type TNode struct {
	Id   chotki.ID
	Host *chotki.Chotki
}

func (fn *TNode) findType() {
	// db
}

func (fn *TNode) ID() chotki.ID {
	return fn.Id
}
func (fn *TNode) String() string {
	// get
	return ""
}
func (fn *TNode) List() []string {
	return []string{}
}
func (fn *TNode) Get(name string) Node {
	return nil // todo off etc
}
func (fn *TNode) Put(loc string, node Node) error {
	return chotki.ErrNotImplemented
}
func (fn *TNode) Set(val string) error {
	return chotki.ErrNotImplemented
}

// REPL per se.
type REPL struct {
	Host chotki.Chotki
	rl   *readline.Instance
	Root Node
}

func (repl *REPL) Map(ctx chotki.ID, path string) (id chotki.ID, err error) {
	// recur
	return chotki.ID0, nil
}

func (repl *REPL) Name(path, name string) error {
	return nil
}

func (repl *REPL) Get(path string) error {
	return nil
}

func (repl *REPL) Set(path, value string) error {
	return nil
}

func (repl *REPL) Put(path, node string) error {
	return nil
}

func (repl *REPL) List(path string) error {
	return nil
}

var completer = readline.NewPrefixCompleter(
	readline.PcItem("help"),

	readline.PcItem("listen"),
	readline.PcItem("talk"),
	readline.PcItem("mute"),
	readline.PcItem("bye"),

	readline.PcItem("name"),

	readline.PcItem("get"),
	readline.PcItem("list"),
	readline.PcItem("put"),
	readline.PcItem("set"),

	readline.PcItem("exit"),
	readline.PcItem("quit"),
)

func filterInput(r rune) (rune, bool) {
	switch r {
	// block CtrlZ feature
	case readline.CharCtrlZ:
		return r, false
	}
	return r, true
}

func (repl *REPL) Open() (err error) {
	repl.rl, err = readline.NewEx(&readline.Config{
		Prompt:          "◌ ", //"\033[31m◌\033[0m ",
		HistoryFile:     ".chotki_cmd_log.txt",
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",

		HistorySearchFold:   true,
		FuncFilterInputRune: filterInput,
	})
	if err != nil {
		return
	}
	repl.rl.CaptureExitSignal()
	return
}

func (repl *REPL) Close() error {
	if repl.rl != nil {
		_ = repl.rl.Close()
		repl.rl = nil
	}
	return nil
}

func (repl *REPL) REPL() (err error) {
	var line string
	line, err = repl.rl.Readline()
	if err == readline.ErrInterrupt && len(line) != 0 {
		return nil
	}
	if err != nil {
		return err
	}

	line = strings.TrimSpace(line)
	cmd, path, arg, err := rdx.ParseREPL([]byte(line))
	if err != nil {
		return
	}
	switch cmd {
	case "listen":
		fmt.Println("I am listening")
	case "create":
		err = repl.CommandCreate(path, arg)
	case "open":
		err = repl.CommandOpen(path, arg)
	case "close":
		err = repl.CommandClose(path, arg)
	case "exit", "quit":
		err = io.EOF
	case "new":
		err = repl.CommandNew(path, arg)
	case "ping":
		// args[1] is an object/field id (otherwise create)
		// subscribe to evs
		// start
	case "pong":
		// args[1] is an object/field
		// subscribe
	case "show", "list":
	default:
		_, _ = fmt.Fprintf(os.Stderr, "command unknown: %s\n", cmd)
	}
	return
}

func main() {

	/*
		if len(os.Args) > 1 {
			rno := uint64(1)
			_, err := fmt.Sscanf(os.Args[1], "%d", &rno)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "Usage: Chotki 123")
				os.Exit(-2)
			}
			err = re.Open(rno)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(-1)
			}
		}*/

	repl := REPL{}

	err := repl.Open()

	for err != io.EOF {
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			err = nil
		}
		err = repl.REPL()
	}

	return
}
