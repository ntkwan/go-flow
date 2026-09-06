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

func TestAllocWhenZeroAlloc(t *testing.T) {
	base := flow.Step[context.Context](func(ctx context.Context) error { return nil })
	step := base.When(func(ctx context.Context) bool { return true })
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for When, got %f", allocs)
	}
}

func TestAllocUnlessZeroAlloc(t *testing.T) {
	base := flow.Step[context.Context](func(ctx context.Context) error { return nil })
	step := base.Unless(func(ctx context.Context) bool { return false })
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Unless, got %f", allocs)
	}
}
func TestAllocWrapZeroAlloc(t *testing.T) {
	base := flow.Step[context.Context](func(ctx context.Context) error { return nil })
	step := base.Wrap(func(next flow.Step[context.Context]) flow.Step[context.Context] {
		return func(ctx context.Context) error {
			return next(ctx)
		}
	})
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Wrap, got %f", allocs)
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

func TestAllocDAGBudget(t *testing.T) {
	nA := flow.Node("A", func(ctx context.Context) error { return nil })
	nB := flow.Node("B", func(ctx context.Context) error { return nil }).After("A")
	nC := flow.Node("C", func(ctx context.Context) error { return nil }).After("B")
	step := flow.DAG(nA, nB, nC)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs > 15 {
		t.Fatalf("expected <= 15 allocs/op for 3-node DAG, got %f", allocs)
	}
}

func TestAllocDynamicZeroAlloc(t *testing.T) {
	inner := flow.Step[context.Context](func(ctx context.Context) error { return nil })
	step := flow.Dynamic(func(ctx context.Context) flow.Step[context.Context] {
		return inner
	})
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Dynamic, got %f", allocs)
	}
}

func TestAllocGoBudget(t *testing.T) {
	step := flow.Go(
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
	)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs > 10 {
		t.Fatalf("expected <= 10 allocs/op for Go(3), got %f", allocs)
	}
}

func TestAllocGoNBudget(t *testing.T) {
	steps := make([]flow.Step[context.Context], 10)
	for i := range steps {
		steps[i] = func(ctx context.Context) error { return nil }
	}
	step := flow.GoN(4, steps...)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs > 12 {
		t.Fatalf("expected <= 12 allocs/op for GoN(4, 10 steps), got %f", allocs)
	}
}

func TestAllocDAGDiamondBudget(t *testing.T) {
	nA := flow.Node("A", func(ctx context.Context) error { return nil })
	nB := flow.Node("B", func(ctx context.Context) error { return nil }).After("A")
	nC := flow.Node("C", func(ctx context.Context) error { return nil }).After("A")
	nD := flow.Node("D", func(ctx context.Context) error { return nil }).After("B", "C")
	step := flow.DAG(nA, nB, nC, nD)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs > 10 {
		t.Fatalf("expected <= 10 allocs/op for Diamond DAG, got %f", allocs)
	}
}

func TestAllocDAGNBudget(t *testing.T) {
	nA := flow.Node("A", func(ctx context.Context) error { return nil })
	nB := flow.Node("B", func(ctx context.Context) error { return nil }).After("A")
	nC := flow.Node("C", func(ctx context.Context) error { return nil }).After("A")
	nD := flow.Node("D", func(ctx context.Context) error { return nil }).After("B", "C")
	step := flow.DAGN(2, nA, nB, nC, nD)
	ctx := context.Background()

	allocs := testing.AllocsPerRun(100, func() {
		_ = step(ctx)
	})

	if allocs > 12 {
		t.Fatalf("expected <= 12 allocs/op for Diamond DAGN, got %f", allocs)
	}
}
