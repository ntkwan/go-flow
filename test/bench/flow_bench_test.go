package bench_test

import (
	"context"
	"testing"
	"time"

	"github.com/ntkwan/go-flow"
)

func BenchmarkSeq(b *testing.B) {
	step := flow.Seq(
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
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

func BenchmarkGo(b *testing.B) {
	step := flow.Go(
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
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

func BenchmarkGoN(b *testing.B) {
	steps := make([]flow.Step[context.Context], 10)
	for i := range steps {
		steps[i] = func(ctx context.Context) error { return nil }
	}
	step := flow.GoN(4, steps...)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRace(b *testing.B) {
	step := flow.Race(
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
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

func BenchmarkRetry(b *testing.B) {
	base := flow.Step[context.Context](func(ctx context.Context) error { return nil })
	step := base.Retry(3, 0)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTimeout(b *testing.B) {
	base := flow.Step[context.Context](func(ctx context.Context) error { return nil })
	step := base.Timeout(5 * time.Second)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBranch(b *testing.B) {
	step := flow.Branch(
		func(ctx context.Context) bool { return true },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
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
