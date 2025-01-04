package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/ergochat/readline"
)

// REPL per se.
type REPL struct {
	Host *chotki.Chotki
	rl   *readline.Instance
	snap pebble.Reader
}

var ErrBadPath = errors.New("bad path")

// todo synchronize these automatically, maybe use a script
var completer = readline.NewPrefixCompleter(
	readline.PcItem("help"),

	readline.PcItem("create"),
	readline.PcItem("open"),
	readline.PcItem("close"),
	readline.PcItem("exit"),
	readline.PcItem("quit"),

	readline.PcItem("listen"),
	readline.PcItem("connect"),

	readline.PcItem("class"),
	readline.PcItem("new"),
	readline.PcItem("edit"),
	readline.PcItem("cat"),
	readline.PcItem("list"),

	readline.PcItem("name"),

	readline.PcItem("dump",
		readline.PcItem("objects"),
		readline.PcItem("vv"),
		readline.PcItem("all"),
	),

	readline.PcItem("tell"),
	readline.PcItem("mute"),

	readline.PcItem("ping"),
	readline.PcItem("pong"),
	readline.PcItem("pinc"),
	readline.PcItem("ponc"),
	readline.PcItem("tinc"),
	readline.PcItem("sinc"),

	readline.PcItem("swagger"),
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

func (repl *REPL) REPL(line string) (id rdx.ID, err error) {

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
	if repl.snap != nil {
		_ = repl.snap.Close()
		repl.snap = nil
	}
	if repl.Host != nil && repl.Host.Last() != rdx.ID0 {
		repl.snap = repl.Host.Snapshot()
	}
	switch cmd {
	// replica open/close
	case "create":
		id, err = repl.CommandCreate(arg)
	case "open":
		id, err = repl.CommandOpen(arg)
	case "opendir":
		id, err = repl.CommandOpenDir(arg)
	case "swagger":
		id, err = repl.CommandSwagger(arg)
	case "servehttp":
		id, err = repl.CommandServeHttp(arg)
	case "checkpoint", "cp":
		id, err = repl.CommandCheckpoint(arg)
	case "close":
		id, err = repl.CommandClose(arg)
	case "exit", "quit":
		if repl.Host != nil && repl.Host.Last() != rdx.ID0 {
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
	case "name":
		id, err = repl.CommandName(arg)
	case "inc":
		id, err = repl.CommandInc(arg)
	case "add":
		id, err = repl.CommandAdd(arg)
	case "choc":
		id, err = repl.CommandCompile(arg)
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
		id, err = repl.CommandPonc(arg)
	case "mute":
		id, err = repl.CommandMute(arg)
	case "tinc":
		id, err = repl.CommandTinc(arg)
	case "sinc":
		id, err = repl.CommandSinc(arg)
	case "valid":
		id, err = repl.CommandValid(arg)
	case "whosaw":
		id, err = repl.CommandWhoSaw(arg)
	default:
		_, _ = fmt.Fprintf(os.Stderr, "command unknown: %s\n", cmd)
	}
	return
}

func report(id rdx.ID, err error) {
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		err = nil
	} else if id != rdx.ID0 {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", id.String())
	}
}

func main() {

	repl := REPL{}

	err := repl.Open()
	var id rdx.ID

	if len(os.Args) > 1 {
		cmds := []string{}
		cmd := ""
		for _, arg := range os.Args[1:] {
			if len(arg) > 0 && arg[len(arg)-1] == ',' {
				cmd = cmd + " " + arg[:len(arg)-1]
				cmds = append(cmds, cmd)
				cmd = ""
			} else {
				cmd = cmd + " " + arg
			}
		}
		if len(cmd) > 0 {
			cmds = append(cmds, cmd)
		}
		for i := 0; i < len(cmds) && err == nil; i++ {
			fmt.Fprintf(os.Stderr, "◌ %s\n", cmds[i])
			id, err = repl.REPL(cmds[i])
			report(id, err)
		}
	}

	for err != io.EOF {
		report(id, err)

		var line string
		line, err = repl.rl.Readline()
		if err == readline.ErrInterrupt && len(line) != 0 {
			id = rdx.BadId
			err = nil
			continue
		}
		if err != nil {
			id = rdx.BadId
			continue
		}

		id, err = repl.REPL(line)
	}

}
