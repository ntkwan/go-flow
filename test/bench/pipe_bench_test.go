package bench_test

import (
	"context"
	"slices"
	"testing"

	"github.com/ntkwan/go-flow"
)

func BenchmarkPipe(b *testing.B) {
	type State struct {
		Val int
	}
	st := &State{Val: 10}
	step := flow.Pipe(
		func(ctx context.Context, in int) (int, error) { return in * 2, nil },
		func(ctx context.Context) int { return st.Val },
		func(ctx context.Context, out int) { st.Val = out },
	)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPipeSeq(b *testing.B) {
	items := slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8})
	step := flow.PipeSeq(
		items,
		func(ctx context.Context, in int) (int, error) { return in * 2, nil },
		func(ctx context.Context, out int) error { return nil },
	)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
