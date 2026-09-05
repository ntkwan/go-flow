package alloc_test

import (
	"context"
	"testing"

	"github.com/ntkwan/go-flow"
)

func TestAllocSeqZeroAlloc(t *testing.T) {
	step := flow.Seq(
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
	)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Seq, got %f", allocs)
	}
}

func TestAllocBranchZeroAlloc(t *testing.T) {
	step := flow.Branch(
		func(ctx context.Context) bool { return true },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
	)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Branch, got %f", allocs)
	}
}

func TestAllocPipeZeroAlloc(t *testing.T) {
	type State struct {
		Val int
	}
	st := &State{Val: 42}
	step := flow.Pipe(
		func(ctx context.Context, in int) (int, error) {
			return in + 1, nil
		},
		func(ctx context.Context) int {
			return st.Val
		},
		func(ctx context.Context, out int) {
			st.Val = out
		},
	)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Pipe, got %f", allocs)
	}
}
