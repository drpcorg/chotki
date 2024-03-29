.PHONY: test
test:
	go test -race ./...

.PHONY: lint
lint:
	golangci-lint run ./...
