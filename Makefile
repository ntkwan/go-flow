.PHONY: all test test-race cover fmt lint lint-actions check bench clean

all: check lint lint-actions test-race

test:
	go test -v ./...

test-race:
	go test -v -race ./...

cover:
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

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

clean:
	rm -f coverage.out
