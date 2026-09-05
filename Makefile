.PHONY: all test test-race stress leak cover bdd uat fmt lint lint-actions check bench bench-compare fuzz mutation clean gen

all: check lint lint-actions test-race leak bdd

gen:
	go run ./cmd/gen-diagrams

test:
	go test -v ./...

test-race:
	go test -v -race ./...

stress:
	go test -v -race -count=100 -run '^TestStress' ./...

leak:
	cd test/leak && go test -v ./...

fuzz:
	go test -fuzz=FuzzDAG -fuzztime=5s ./test/fuzz
	go test -fuzz=FuzzChunk -fuzztime=5s ./test/fuzz
	go test -fuzz=FuzzPipeSeq -fuzztime=5s ./test/fuzz

cover:
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

bdd:
	cd test/bdd && go test -v ./...

uat: bdd

fmt:
	gofmt -w -s .

lint:
	golangci-lint run ./...

lint-actions:
	@if command -v actionlint >/dev/null 2>&1; then \
		actionlint; \
	else \
		echo "actionlint not installed, skipping..."; \
	fi

check:
	@UNFORMATTED=$$(gofmt -l .); \
	if [ -n "$$UNFORMATTED" ]; then \
		echo "Unformatted files found:"; \
		echo "$$UNFORMATTED"; \
		exit 1; \
	fi
	go vet ./...

bench:
	go test -v -bench=. -benchmem -run=^$$ ./...

bench-compare:
	@go test -bench=. -benchmem -count=5 -run=^$$ ./... > new.bench
	@if [ -f old.bench ]; then \
		if command -v benchstat >/dev/null 2>&1; then \
			benchstat old.bench new.bench; \
		else \
			echo "benchstat not installed. Install via: go install golang.org/x/perf/cmd/benchstat@latest"; \
		fi \
	else \
		echo "old.bench not found. Renaming new.bench to old.bench as baseline."; \
		mv new.bench old.bench; \
	fi

mutation:
	@if command -v gremlins >/dev/null 2>&1; then \
		gremlins unleash --exclude-files 'examples/.*' --exclude-files 'cmd/.*' --exclude-files 'test/.*'; \
	elif command -v go-mutesting >/dev/null 2>&1; then \
		go-mutesting ./...; \
	else \
		echo "Neither gremlins nor go-mutesting is installed. Install via: go install github.com/go-gremlins/gremlins/cmd/gremlins@latest or go install github.com/zimmski/go-mutesting/cmd/go-mutesting@latest"; \
	fi

clean:
	rm -f coverage.out new.bench old.bench

