package main

import (
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/ergochat/readline"
	"github.com/learn-decentralized-systems/toytlv"
	"io"
	"os"
	"strings"
)

// REPL per se.
type REPL struct {
	Host chotki.Chotki
	tcp  *toytlv.TCPDepot
	rl   *readline.Instance
	snap pebble.Reader
}

var ErrBadPath = errors.New("bad path")

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
	if len(line) == 0 {
		return rdx.ID0, nil
	}
	ws := strings.IndexAny(line, " \t\r\n")
	cmd := ""
	if ws > 0 {
		cmd = line[:ws]
		line = strings.TrimSpace(line[ws:])
	} else {
		cmd = line
		line = ""
	}
	var arg *rdx.RDX
	arg, err = rdx.ParseRDX([]byte(line))
	if err != nil {
		return rdx.ID0, err
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
		id, err = repl.CommandCreate(arg)
	case "open":
		id, err = repl.CommandOpen(arg)
	case "close":
		id, err = repl.CommandClose(arg)
	case "exit", "quit":
		if repl.Host.Last() != rdx.ID0 {
			id, err = repl.CommandClose(arg)
		}
		if err == nil {
			err = io.EOF
		}
	// ----- object handling -----
	case "class":
		id, err = repl.CommandClass(arg)
	case "new":
		id, err = repl.CommandNew(arg)
	case "edit":
		id, err = repl.CommandEdit(arg)
	case "ls", "show", "list":
		id, err = repl.CommandList(arg)
	case "cat":
		id, err = repl.CommandCat(arg)
	// ----- networking -----
	case "listen":
		id, err = repl.CommandListen(arg)
	case "connect":
		id, err = repl.CommandConnect(arg)
	// ----- debug -----
	case "dump":
		id, err = repl.CommandDump(arg)
	case "tell":
		id, err = repl.CommandTell(arg)
	case "ping":
		id, err = repl.CommandPing(arg)
	case "pong":
		// args[1] is an object/field
		// subscribe
	case "pinc":
		id, err = repl.CommandPinc(arg)
	case "ponc":
		id, err = repl.CommandPinc(arg)
	case "tic":
		id, err = repl.CommandTic(arg)
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
