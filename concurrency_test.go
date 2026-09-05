package flow

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestGoNEmpty(t *testing.T) {
	step := GoN[context.Context](3)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestGoNSuccess(t *testing.T) {
	var count atomic.Int32
	steps := make([]Step[context.Context], 20)
	for i := range steps {
		steps[i] = func(ctx context.Context) error {
			count.Add(1)
			return nil
		}
	}

	step := GoN(4, steps...)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if count.Load() != 20 {
		t.Fatalf("expected 20 executions, got %d", count.Load())
	}
}

func TestGoNLimitZeroOrNegative(t *testing.T) {
	var count atomic.Int32
	step := GoN(
		0,
		func(ctx context.Context) error {
			count.Add(1)
			return nil
		},
		func(ctx context.Context) error {
			count.Add(1)
			return nil
		},
	)

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if count.Load() != 20 && count.Load() != 2 {
		t.Fatalf("expected 2 executions, got %d", count.Load())
	}
}

func TestGoNConcurrencyBound(t *testing.T) {
	const limit = 3
	const total = 15

	var currentActive atomic.Int32
	var maxActive atomic.Int32

	steps := make([]Step[context.Context], total)
	for i := range steps {
		steps[i] = func(ctx context.Context) error {
			active := currentActive.Add(1)
			for {
				oldMax := maxActive.Load()
				if active <= oldMax || maxActive.CompareAndSwap(oldMax, active) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			currentActive.Add(-1)
			return nil
		}
	}

	step := GoN(limit, steps...)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if maxActive.Load() > limit {
		t.Fatalf("expected max active goroutines <= %d, got %d", limit, maxActive.Load())
	}
}

func TestGoNError(t *testing.T) {
	errFail1 := errors.New("err 1")
	errFail2 := errors.New("err 2")

	step := GoN(
		2,
		func(ctx context.Context) error { return errFail1 },
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return errFail2 },
	)

	err := step(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errFail1) || !errors.Is(err, errFail2) {
		t.Fatalf("expected both errors in %v", err)
	}
}

func TestRaceEmpty(t *testing.T) {
	step := Race[context.Context]()
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRaceFastestSuccess(t *testing.T) {
	var canceled atomic.Bool

	step := Race(
		func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		func(ctx context.Context) error {
			select {
			case <-time.After(200 * time.Millisecond):
				return nil
			case <-ctx.Done():
				canceled.Store(true)
				return ctx.Err()
			}
		},
	)

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if !canceled.Load() {
		t.Fatal("expected slower branch to be canceled")
	}
}

func TestRaceAllFail(t *testing.T) {
	err1 := errors.New("branch 1 fail")
	err2 := errors.New("branch 2 fail")

	step := Race(
		func(ctx context.Context) error {
			return err1
		},
		func(ctx context.Context) error {
			return err2
		},
	)

	err := step(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, err1) || !errors.Is(err, err2) {
		t.Fatalf("expected combined errors, got %v", err)
	}
}

func TestRaceParentContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	step := Race(
		func(c context.Context) error {
			<-c.Done()
			return c.Err()
		},
	)

	err := step(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestEachEmpty(t *testing.T) {
	var count int
	seq := slices.Values([]int{})
	step := Each(seq, func(ctx context.Context, item int) error {
		count++
		return nil
	})

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 items, got %d", count)
	}
}

func TestEachSuccess(t *testing.T) {
	var items []string
	seq := slices.Values([]string{"a", "b", "c"})
	step := Each(seq, func(ctx context.Context, item string) error {
		items = append(items, item)
		return nil
	})

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(items) != 3 || items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Fatalf("expected [a b c], got %v", items)
	}
}

func TestEachShortCircuit(t *testing.T) {
	var items []int
	errFail := errors.New("stop at 2")
	seq := slices.Values([]int{1, 2, 3, 4})

	step := Each(seq, func(ctx context.Context, item int) error {
		items = append(items, item)
		if item == 2 {
			return errFail
		}
		return nil
	})

	err := step(context.Background())
	if !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}
	if len(items) != 2 || items[0] != 1 || items[1] != 2 {
		t.Fatalf("expected [1 2], got %v", items)
	}
}

func TestEachNilSeq(t *testing.T) {
	step := Each[context.Context, int](nil, func(ctx context.Context, item int) error {
		return nil
	})

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestEach2Empty(t *testing.T) {
	var count int
	seq := func(yield func(string, int) bool) {}
	step := Each2(seq, func(ctx context.Context, k string, v int) error {
		count++
		return nil
	})

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 items, got %d", count)
	}
}

func TestEach2Success(t *testing.T) {
	items := make(map[string]int)
	seq := func(yield func(string, int) bool) {
		if !yield("a", 1) {
			return
		}
		if !yield("b", 2) {
			return
		}
	}
	step := Each2(seq, func(ctx context.Context, k string, v int) error {
		items[k] = v
		return nil
	})

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(items) != 2 || items["a"] != 1 || items["b"] != 2 {
		t.Fatalf("expected map[a:1 b:2], got %v", items)
	}
}

func TestEach2ShortCircuit(t *testing.T) {
	errStop := errors.New("stop at 2")
	seq := func(yield func(string, int) bool) {
		if !yield("a", 1) {
			return
		}
		if !yield("b", 2) {
			return
		}
		yield("c", 3)
	}

	var visited []string
	step := Each2(seq, func(ctx context.Context, k string, v int) error {
		visited = append(visited, k)
		if v == 2 {
			return errStop
		}
		return nil
	})

	err := step(context.Background())
	if !errors.Is(err, errStop) {
		t.Fatalf("expected %v, got %v", errStop, err)
	}
	if len(visited) != 2 || visited[0] != "a" || visited[1] != "b" {
		t.Fatalf("expected [a b], got %v", visited)
	}
}

func TestEach2NilSeq(t *testing.T) {
	step := Each2[context.Context, string, int](nil, func(ctx context.Context, k string, v int) error {
		return nil
	})

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestOnce(t *testing.T) {
	var count atomic.Int32
	baseStep := func(ctx context.Context) error {
		count.Add(1)
		return nil
	}

	onceStep := Once(baseStep)

	for range 5 {
		if err := onceStep(context.Background()); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	}

	if count.Load() != 1 {
		t.Fatalf("expected exactly 1 execution, got %d", count.Load())
	}
}

func TestOnceNil(t *testing.T) {
	step := Once[context.Context](nil)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestOnceError(t *testing.T) {
	errFail := errors.New("fail once")
	var count atomic.Int32
	onceStep := Once(func(ctx context.Context) error {
		count.Add(1)
		return errFail
	})

	for range 3 {
		err := onceStep(context.Background())
		if !errors.Is(err, errFail) {
			t.Fatalf("expected %v, got %v", errFail, err)
		}
	}

	if count.Load() != 1 {
		t.Fatalf("expected exactly 1 execution, got %d", count.Load())
	}
}

func BenchmarkGoN(b *testing.B) {
	steps := make([]Step[context.Context], 10)
	for i := range steps {
		steps[i] = func(ctx context.Context) error { return nil }
	}
	step := GoN(4, steps...)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = step(ctx)
	}
}

func BenchmarkRace(b *testing.B) {
	step := Race(
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return nil },
	)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = step(ctx)
	}
}

func BenchmarkEach(b *testing.B) {
	data := []int{1, 2, 3, 4, 5}
	step := Each(slices.Values(data), func(ctx context.Context, item int) error {
		return nil
	})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = step(ctx)
	}
}

func TestChunk(t *testing.T) {
	stepNil := Chunk[context.Context, int](nil, 2, func(ctx context.Context, batch []int) error {
		return nil
	})
	if err := stepNil(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil seq, got %v", err)
	}

	stepNilFunc := Chunk[context.Context, int](slices.Values([]int{1, 2}), 2, nil)
	if err := stepNilFunc(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil func, got %v", err)
	}

	var batches [][]int
	step := Chunk(slices.Values([]int{1, 2, 3, 4, 5}), 2, func(ctx context.Context, batch []int) error {
		batches = append(batches, batch)
		return nil
	})

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if len(batches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[1]) != 2 || len(batches[2]) != 1 {
		t.Fatalf("unexpected batch sizes: %v", batches)
	}

	stepZeroSize := Chunk(slices.Values([]int{10, 20}), 0, func(ctx context.Context, batch []int) error {
		if len(batch) != 1 {
			t.Fatalf("expected size 1 batch for zero limit, got %d", len(batch))
		}
		return nil
	})
	if err := stepZeroSize(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	errFail := errors.New("fail on batch 2")
	stepErr := Chunk(slices.Values([]int{1, 2, 3, 4}), 2, func(ctx context.Context, batch []int) error {
		if batch[0] == 3 {
			return errFail
		}
		return nil
	})
	if err := stepErr(context.Background()); !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}

	stepErrLast := Chunk(slices.Values([]int{1, 2, 3}), 2, func(ctx context.Context, batch []int) error {
		if batch[0] == 3 {
			return errFail
		}
		return nil
	})
	if err := stepErrLast(context.Background()); !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}
}

func TestChunk2(t *testing.T) {
	stepNil := Chunk2[context.Context, string, int](nil, 2, func(ctx context.Context, keys []string, vals []int) error {
		return nil
	})
	if err := stepNil(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil seq, got %v", err)
	}

	stepNilFunc := Chunk2[context.Context, string, int](func(yield func(string, int) bool) {}, 2, nil)
	if err := stepNilFunc(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil func, got %v", err)
	}

	seq := func(yield func(string, int) bool) {
		if !yield("a", 1) {
			return
		}
		if !yield("b", 2) {
			return
		}
		if !yield("c", 3) {
			return
		}
	}

	var collectedKeys [][]string
	var collectedVals [][]int
	step := Chunk2(seq, 2, func(ctx context.Context, keys []string, vals []int) error {
		collectedKeys = append(collectedKeys, keys)
		collectedVals = append(collectedVals, vals)
		return nil
	})

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(collectedKeys) != 2 || len(collectedVals) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(collectedKeys))
	}

	stepZeroSize := Chunk2(seq, 0, func(ctx context.Context, keys []string, vals []int) error {
		if len(keys) != 1 {
			t.Fatalf("expected size 1, got %d", len(keys))
		}
		return nil
	})
	if err := stepZeroSize(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	errFail := errors.New("fail on batch 1")
	stepErr := Chunk2(seq, 2, func(ctx context.Context, keys []string, vals []int) error {
		if len(keys) == 2 {
			return errFail
		}
		return nil
	})
	if err := stepErr(context.Background()); !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}

	stepErrLast := Chunk2(seq, 2, func(ctx context.Context, keys []string, vals []int) error {
		if len(keys) == 1 {
			return errFail
		}
		return nil
	})
	if err := stepErrLast(context.Background()); !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}
}
