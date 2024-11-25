package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/drpcorg/chotki/rdx"
)

func AddCorsHeaders(f func(w http.ResponseWriter, req *http.Request)) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Max-Age", "86400")
		f(w, req)
	}
}

func ListenHandler(repl *REPL) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		switch method := req.Method; method {
		case "OPTIONS":
			w.Header().Set("Access-Control-Allow-Methods", "POST")
			w.WriteHeader(http.StatusNoContent)
		case "POST":
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			arg, err := rdx.ParseRDX(body)
			if arg == nil || arg.RdxType != rdx.String {
				http.Error(w, fmt.Sprintf("Argument must be string"), http.StatusUnprocessableEntity)
				return
			}
			_, err = repl.CommandListen(arg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, fmt.Sprintf("Unsupported method %s", req.Method), http.StatusMethodNotAllowed)
		}
	}
}

func ConnectHandler(repl *REPL) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		switch method := req.Method; method {
		case "OPTIONS":
			w.Header().Set("Access-Control-Allow-Methods", "POST")
			w.WriteHeader(http.StatusNoContent)
		case "POST":
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			arg, err := rdx.ParseRDX(body)
			if arg == nil || arg.RdxType != rdx.String {
				http.Error(w, fmt.Sprintf("Argument must be string"), http.StatusUnprocessableEntity)
				return
			}
			_, err = repl.CommandConnect(arg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, fmt.Sprintf("Unsupported method %s", req.Method), http.StatusMethodNotAllowed)
		}
	}
}

func ClassHandler(repl *REPL) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		switch method := req.Method; method {
		case "OPTIONS":
			w.Header().Set("Access-Control-Allow-Methods", "POST")
			w.WriteHeader(http.StatusNoContent)
		case "POST":
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			arg, err := rdx.ParseRDX(body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if arg == nil || arg.RdxType != rdx.Mapping {
				http.Error(w, fmt.Sprintf("Argument must be mapping"), http.StatusUnprocessableEntity)
				return
			}
			id, err := repl.CommandClass(arg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(id.String()))
		default:
			http.Error(w, fmt.Sprintf("Unsupported method %s", req.Method), http.StatusMethodNotAllowed)
		}
	}
}

func NameHandler(repl *REPL) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		switch method := req.Method; method {
		case "OPTIONS":
			w.Header().Set("Access-Control-Allow-Methods", "PUT")
			w.WriteHeader(http.StatusNoContent)
		case "PUT":
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			arg, err := rdx.ParseRDX(body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if arg == nil || arg.RdxType != rdx.Mapping {
				http.Error(w, fmt.Sprintf("Argument must be mapping"), http.StatusUnprocessableEntity)
				return
			}
			id, err := repl.CommandName(arg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(id.String()))
		default:
			http.Error(w, fmt.Sprintf("Unsupported method %s", req.Method), http.StatusMethodNotAllowed)
		}
	}
}

func NewHandler(repl *REPL) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		switch method := req.Method; method {
		case "OPTIONS":
			w.Header().Set("Access-Control-Allow-Methods", "POST")
			w.WriteHeader(http.StatusNoContent)
		case "POST":
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			arg, err := rdx.ParseRDX(body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if arg == nil || arg.RdxType != rdx.Mapping {
				http.Error(w, fmt.Sprintf("Argument must be mapping"), http.StatusUnprocessableEntity)
				return
			}
			id, err := repl.CommandNew(arg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(id.String()))
		default:
			http.Error(w, fmt.Sprintf("Unsupported method %s", req.Method), http.StatusMethodNotAllowed)
		}
	}
}

func EditHandler(repl *REPL) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		switch method := req.Method; method {
		case "OPTIONS":
			w.Header().Set("Access-Control-Allow-Methods", "PUT")
			w.WriteHeader(http.StatusNoContent)
		case "PUT":
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			arg, err := rdx.ParseRDX(body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if arg == nil || arg.RdxType != rdx.Mapping {
				http.Error(w, fmt.Sprintf("Argument must be eulerian"), http.StatusUnprocessableEntity)
				return
			}
			id, err := repl.CommandEdit(arg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(id.String()))
		default:
			http.Error(w, fmt.Sprintf("Unsupported method %s", req.Method), http.StatusMethodNotAllowed)
		}
	}
}

func CatHandler(repl *REPL) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		switch method := req.Method; method {
		case "OPTIONS":
			w.Header().Set("Access-Control-Allow-Methods", "GET")
			w.WriteHeader(http.StatusNoContent)
		case "GET":
			id := req.URL.Query().Get("id")
			oid := rdx.IDFromText([]byte(id))

			txt, err := repl.Host.ObjectString(oid)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(txt))
		default:
			http.Error(w, fmt.Sprintf("Unsupported method %s", req.Method), http.StatusMethodNotAllowed)
		}
	}
}

func ListHandler(repl *REPL) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		switch method := req.Method; method {
		case "OPTIONS":
			w.Header().Set("Access-Control-Allow-Methods", "GET")
			w.WriteHeader(http.StatusNoContent)
		case "GET":
			id := req.URL.Query().Get("id")
			oid := rdx.IDFromText([]byte(id))

			strs, err := repl.ListObject(oid)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(strings.Join(strs, "")))
		default:
			http.Error(w, fmt.Sprintf("Unsupported method %s", req.Method), http.StatusMethodNotAllowed)
		}
	}
}
