.PHONY: test
test:
	go test -race ./...

build:
	go build ./...

fmt:
	go fmt ./...

ragel:
	cd rdx && go generate && go fmt && go test

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: tlsgen
tlsgen:
	openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 1

.PHONY: update-pebble
update-pebble:
	go mod edit -replace github.com/cockroachdb/pebble=github.com/drpcorg/pebble@master
	go mod tidy

.PHONY: lint
all: ragel fmt build test lint
