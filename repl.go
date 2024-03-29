package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ergochat/readline"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
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

func ShowObject(chotki *Chotki, id ID) error {
	i := chotki.ObjectIterator(id)
	for i.Valid() {
		id, rdt := OKeyIdRdt(i.Key())
		_, _ = fmt.Fprintf(os.Stderr, "%c#%d\t\n", rdt, id.Off())
	}
	return nil
}

var ErrBadObjectJson = errors.New("bad JSON object serialization")
var ErrUnsupportedType = errors.New("unsupported field type")

func CreateObjectFromList(chotki *Chotki, list []interface{}) (id ID, err error) {
	packet := toyqueue.Records{}
	// todo ref type json
	// todo add id, ref
	for _, f := range list {
		var rdt byte
		var body []byte
		switch f.(type) {
		case int64:
			rdt = 'C'
			body = CState(f.(int64))
		case float64:
			rdt = 'N'
		case string:
			str := f.(string)
			id := ParseBracketedID([]byte(str))
			if id != BadId { // check for id-ness
				rdt = 'L'
				body = LState(id, 0)
			} else {
				rdt = 'S'
				body = Stlv(str)
			}
		default:
			err = ErrUnsupportedType
			return
		}
		packet = append(packet, toytlv.Record(rdt, body))
	}
	return chotki.CommitPacket('O', ID0, packet)
}

// ["{10-4f8-0}", +1, "string", 1.0, ...]
func CreateObject(chotki *Chotki, jsn []byte) (id ID, err error) {
	var parsed interface{}
	err = json.Unmarshal(jsn, &parsed)
	if err != nil {
		return
	}
	switch parsed.(type) {
	case []interface{}:
		id, err = CreateObjectFromList(chotki, parsed.([]interface{}))
	default:
		err = ErrBadObjectJson
	}
	return
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
		args = args[1:]
		err = nil
		switch cmd {
		case "listen":
			fmt.Println("I am listening")
		case "exit", "quit":
			ex := 0
			err = re.Close()
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err.Error())
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
			for _, arg := range args {
				id := ParseIDString(arg)
				if id == BadId {
					_, _ = fmt.Fprintf(os.Stderr, "bad id %s\n", arg)
					break
				}
				err = ShowObject(&re, id)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err.Error())
				}
			}
		default:
			_, _ = fmt.Fprintf(os.Stderr, "command unknown: %s\n", cmd)
		}

		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error executing %s: %s\n", cmd, err.Error())
		}
	}
}
