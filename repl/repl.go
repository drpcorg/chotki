package main

import (
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/ergochat/readline"
	"io"
	"os"
	"strings"
)

// REPL per se.
type REPL struct {
	Host chotki.Chotki
	rl   *readline.Instance
	Root Node
	snap pebble.Reader
}

func (repl *REPL) Map(ctx rdx.ID, path string) (id rdx.ID, err error) {
	// recur
	return rdx.ID0, nil
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

var ErrBadPath = errors.New("bad path")

func (repl *REPL) NodeByPath(path *rdx.RDX) (node Node) {
	if path == nil || path.RdxType != rdx.RdxPath {
		return nil
	}
	node = &ReplicaNode{repl: repl}
	for i := 0; node != nil && i < len(path.Nested); i++ {
		normalized := path.Nested[i].String()
		node = node.Get(normalized)
	}
	return
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

func (repl *REPL) REPL() (id rdx.ID, err error) {
	var line string
	line, err = repl.rl.Readline()
	if err == readline.ErrInterrupt && len(line) != 0 {
		return rdx.BadId, nil
	}
	if err != nil {
		return rdx.BadId, err
	}

	line = strings.TrimSpace(line)
	cmd, path, arg, err := rdx.ParseREPL([]byte(line))
	if err != nil {
		return
	}
	if repl.snap != nil {
		_ = repl.snap.Close()
		repl.snap = nil
	}
	if repl.Host.Last() != rdx.ID0 {
		repl.snap = repl.Host.Snapshot()
	}
	switch cmd {
	// replica open/close
	case "create":
		id, err = repl.CommandCreate(path, arg)
	case "open":
		id, err = repl.CommandOpen(path, arg)
	case "close":
		id, err = repl.CommandClose(path, arg)
	case "exit", "quit":
		if repl.Host.Last() != rdx.ID0 {
			id, err = repl.CommandClose(path, arg)
		}
		if err == nil {
			err = io.EOF
		}
	// ----- object handling -----
	case "new":
		id, err = repl.CommandNew(path, arg)
	case "type":
		id, err = repl.CommandType(path, arg)
	case "ls", "show", "list":
		id, err = repl.CommandList(path, arg)
	case "cat":
		id, err = repl.CommandCat(path, arg)
	// ----- networking -----
	case "listen":
		fmt.Println("I am listening")
	// ----- debug -----
	case "dump":
		id, err = repl.CommandDump(path, arg)
	case "ping":
		// args[1] is an object/field id (otherwise create)
		// subscribe to evs
		// start
	case "pong":
		// args[1] is an object/field
		// subscribe
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
	var id rdx.ID

	for err != io.EOF {
		if err != nil {
			_, _ = fmt.Fprintf(os.Stdout, "%s\n", err.Error())
			err = nil
		} else if id != rdx.ID0 {
			_, _ = fmt.Fprintf(os.Stderr, "%s\n", id.String())
		}
		id, err = repl.REPL()
	}

}
