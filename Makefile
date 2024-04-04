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
