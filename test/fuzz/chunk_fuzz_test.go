package fuzz_test

import (
	"context"
	"slices"
	"testing"

	"github.com/ntkwan/go-flow"
)

func FuzzChunk(f *testing.F) {
	f.Add(10, 3)
	f.Add(0, 1)
	f.Add(5, -1)
	f.Add(100, 100)
	f.Add(7, 2)

	f.Fuzz(func(t *testing.T, count int, chunkSize int) {
		if count < 0 || count > 1000 {
			return
		}

		items := make([]int, count)
		for i := range items {
			items[i] = i
		}

		var collected []int
		seq := slices.Values(items)
		step := flow.Chunk(seq, chunkSize, func(ctx context.Context, batch []int) error {
			collected = append(collected, batch...)
			return nil
		})

		err := step(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(collected) != len(items) {
			t.Fatalf("expected len %d, got %d", len(items), len(collected))
		}

		for i := range items {
			if collected[i] != items[i] {
				t.Fatalf("mismatch at %d: expected %d, got %d", i, items[i], collected[i])
			}
		}
	})
}
