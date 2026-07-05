.PHONY: test
test:
	go test ./...

build:
	go build ./...

fmt:
	go fmt ./...

ragel:
	cd rdx && go generate && go fmt && go test

.PHONY: lint
lint:
	golangci-lint run ./...

# Model-check the replication protocol spec (needs tla2tools.jar, see tla/README.md)
TLA2TOOLS ?= tla2tools.jar
.PHONY: tla
tla:
	cd tla && for cfg in MCFixed MCFixedChurn MCWitness MCPing; do \
		echo "=== $$cfg ==="; \
		java -XX:+UseParallelGC -cp $(TLA2TOOLS) tlc2.TLC -config $$cfg.cfg -workers auto -deadlock MCChotkiSync || true; \
	done

.PHONY: update-pebble
update-pebble:
	go mod edit -replace github.com/cockroachdb/pebble=github.com/drpcorg/pebble@master
	go mod tidy

.PHONY: lint
all: ragel fmt build test lint
