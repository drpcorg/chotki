package main

import (
	"fmt"
	"github.com/ergochat/readline"
	"io"
	"os"
	"strings"
)

var completer = readline.NewPrefixCompleter(
	readline.PcItem("help"),
	readline.PcItem("listen"),
	readline.PcItem("connect"),
	readline.PcItem("exit"),
	readline.PcItem("quit"),
	readline.PcItem("show"),
)

func filterInput(r rune) (rune, bool) {
	switch r {
	// block CtrlZ feature
	case readline.CharCtrlZ:
		return r, false
	}
	return r, true
}

func main() {

	l, err := readline.NewEx(&readline.Config{
		Prompt:          "◌ ", //"\033[31m◌\033[0m ",
		HistoryFile:     "/tmp/readline.tmp",
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",

		HistorySearchFold:   true,
		FuncFilterInputRune: filterInput,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()
	l.CaptureExitSignal()

	re := Chotki{}

	if len(os.Args) > 1 {
		rno := uint32(1)
		_, err := fmt.Sscanf(os.Args[1], "%d", &rno)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Usage: Chotki 123")
			os.Exit(-2)
		}
		err = re.Open(rno)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(-1)
		}
	}

	for {
		line, err := l.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		args := strings.Split(line, " ")
		cmd := args[0]
		err = nil
		switch cmd {
		case "listen":
			fmt.Println("I am listening")
		case "exit", "quit":
			ex := 0
			err = re.Close()
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				ex = -1
			}
			os.Exit(ex)
		case "ping":
			// args[1] is an object/field id (otherwise create)
			// subscribe to evs
			// start
		case "pong":
			// args[1] is an object/field
			// subscribe
		case "show", "list":
			//err = re.ShowAll()
		default:
			fmt.Fprintf(os.Stderr, "command unknown: %s\n", cmd)
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error executing %s: %s\n", cmd, err.Error())
		}
	}
}
