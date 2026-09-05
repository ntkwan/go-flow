module github.com/ntkwan/go-flow/test/leak

go 1.24

replace github.com/ntkwan/go-flow => ../..

require (
	github.com/ntkwan/go-flow v0.0.0-00010101000000-000000000000
	go.uber.org/goleak v1.3.0
)
