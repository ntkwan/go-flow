.PHONY: all test test-race cover fmt check bench clean

all: check test-race

test:
	go test -v ./...

test-race:
	go test -v -race ./...

cover:
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

fmt:
	gofmt -w -s .

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
