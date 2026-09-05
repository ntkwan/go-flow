package flow

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
)

type pipeContext struct {
	context.Context
	Input  int
	Output string
}

func TestPipeSuccess(t *testing.T) {
	ctx := &pipeContext{
		Context: context.Background(),
		Input:   42,
	}

	step := Pipe(
		func(ctx *pipeContext, in int) (string, error) {
			return strconv.Itoa(in * 2), nil
		},
		func(ctx *pipeContext) int {
			return ctx.Input
		},
		func(ctx *pipeContext, out string) {
			ctx.Output = out
		},
	)

	if err := step(ctx); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ctx.Output != "84" {
		t.Fatalf("expected output '84', got %q", ctx.Output)
	}
}

func TestPipeError(t *testing.T) {
	expectedErr := errors.New("pipe transform failure")
	ctx := &pipeContext{
		Context: context.Background(),
		Input:   10,
	}

	step := Pipe(
		func(ctx *pipeContext, in int) (string, error) {
			return "", expectedErr
		},
		func(ctx *pipeContext) int {
			return ctx.Input
		},
		func(ctx *pipeContext, out string) {
			ctx.Output = out
		},
	)

	err := step(ctx)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
	if ctx.Output != "" {
		t.Fatalf("expected empty output on error, got %q", ctx.Output)
	}
}

func TestPipeNilFn(t *testing.T) {
	ctx := context.Background()
	step := Pipe[context.Context, int, string](nil, nil, nil)
	if err := step(ctx); err != nil {
		t.Fatalf("expected nil error for nil fn, got %v", err)
	}
}

func TestPipeSeqSuccess(t *testing.T) {
	items := []int{1, 2, 3, 4, 5}
	var results []string

	step := PipeSeq(
		slices.Values(items),
		func(ctx context.Context, item int) (string, error) {
			return strconv.Itoa(item * 10), nil
		},
		func(ctx context.Context, item string) error {
			results = append(results, item)
			return nil
		},
	)

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	expected := []string{"10", "20", "30", "40", "50"}
	if !slices.Equal(results, expected) {
		t.Fatalf("expected %v, got %v", expected, results)
	}
}

func TestPipeSeqTransformError(t *testing.T) {
	items := []int{1, 2, 3}
	errBoom := errors.New("boom")

	step := PipeSeq(
		slices.Values(items),
		func(ctx context.Context, item int) (string, error) {
			if item == 2 {
				return "", errBoom
			}
			return strconv.Itoa(item), nil
		},
		func(ctx context.Context, item string) error {
			return nil
		},
	)

	err := step(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected error %v, got %v", errBoom, err)
	}
}

func TestPipeSeqSinkError(t *testing.T) {
	items := []int{1, 2, 3}
	errSink := errors.New("sink error")

	step := PipeSeq(
		slices.Values(items),
		func(ctx context.Context, item int) (string, error) {
			return strconv.Itoa(item), nil
		},
		func(ctx context.Context, item string) error {
			if item == "2" {
				return errSink
			}
			return nil
		},
	)

	err := step(context.Background())
	if !errors.Is(err, errSink) {
		t.Fatalf("expected error %v, got %v", errSink, err)
	}
}

func TestPipeSeqNilInputs(t *testing.T) {
	step := PipeSeq[context.Context, int, string](nil, nil, nil)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil inputs, got %v", err)
	}
}

func TestPipeSeq2Success(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2}
	var results []string

	step := PipeSeq2(
		func(yield func(string, int) bool) {
			for k, v := range m {
				if !yield(k, v) {
					return
				}
			}
		},
		func(ctx context.Context, k string, v int) (string, error) {
			return k + ":" + strconv.Itoa(v), nil
		},
		func(ctx context.Context, item string) error {
			results = append(results, item)
			return nil
		},
	)

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 items, got %d", len(results))
	}
}

func TestPipeSeq2Errors(t *testing.T) {
	stepNil := PipeSeq2[context.Context, string, int, string](nil, nil, nil)
	if err := stepNil(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	errTransform := errors.New("transform err")
	seq := func(yield func(string, int) bool) {
		yield("key", 1)
	}
	stepErr1 := PipeSeq2(
		seq,
		func(ctx context.Context, k string, v int) (string, error) {
			return "", errTransform
		},
		func(ctx context.Context, item string) error {
			return nil
		},
	)
	if !errors.Is(stepErr1(context.Background()), errTransform) {
		t.Fatalf("expected %v", errTransform)
	}

	errSink := errors.New("sink err")
	stepErr2 := PipeSeq2(
		seq,
		func(ctx context.Context, k string, v int) (string, error) {
			return "ok", nil
		},
		func(ctx context.Context, item string) error {
			return errSink
		},
	)
	if !errors.Is(stepErr2(context.Background()), errSink) {
		t.Fatalf("expected %v", errSink)
	}
}

func TestPipe2AndPipe3(t *testing.T) {
	ctx := context.Background()

	f1 := func(ctx context.Context, in int) (int, error) {
		return in + 10, nil
	}
	f2 := func(ctx context.Context, in int) (string, error) {
		return strconv.Itoa(in * 2), nil
	}
	f3 := func(ctx context.Context, in string) (bool, error) {
		return len(in) > 2, nil
	}

	p2 := Pipe2(f1, f2)
	res2, err := p2(ctx, 5)
	if err != nil || res2 != "30" {
		t.Fatalf("expected '30', got %q (err=%v)", res2, err)
	}

	p3 := Pipe3(f1, f2, f3)
	res3, err := p3(ctx, 50)
	if err != nil || !res3 {
		t.Fatalf("expected true, got %v (err=%v)", res3, err)
	}

	errFail := errors.New("fail")
	fFail := func(ctx context.Context, in int) (int, error) {
		return 0, errFail
	}
	pFail := Pipe2(fFail, f2)
	_, err = pFail(ctx, 5)
	if !errors.Is(err, errFail) {
		t.Fatalf("expected errFail, got %v", err)
	}

	p3Fail := Pipe3(fFail, f2, f3)
	_, err = p3Fail(ctx, 5)
	if !errors.Is(err, errFail) {
		t.Fatalf("expected errFail, got %v", err)
	}
}

func TestPipeNilCombinations(t *testing.T) {
	ctx := context.Background()
	p2Nil1 := Pipe2[context.Context, int, int, int](nil, func(ctx context.Context, b int) (int, error) {
		return b + 1, nil
	})
	res, err := p2Nil1(ctx, 5)
	if err != nil || res != 1 {
		t.Fatalf("expected 1, got %d", res)
	}

	p2NilAll := Pipe2[context.Context, int, int, int](nil, nil)
	res, err = p2NilAll(ctx, 5)
	if err != nil || res != 0 {
		t.Fatalf("expected 0, got %d", res)
	}

	p2Nil2 := Pipe2[context.Context, int, int, int](func(ctx context.Context, a int) (int, error) {
		return a + 2, nil
	}, nil)
	res, err = p2Nil2(ctx, 5)
	if err != nil || res != 0 {
		t.Fatalf("expected 0, got %d", res)
	}

	p3Nil := Pipe3[context.Context, int, int, int, int](nil, nil, nil)
	res, err = p3Nil(ctx, 5)
	if err != nil || res != 0 {
		t.Fatalf("expected 0, got %d", res)
	}
}

func BenchmarkPipe(b *testing.B) {
	ctx := &pipeContext{
		Context: context.Background(),
		Input:   42,
	}

	step := Pipe(
		func(ctx *pipeContext, in int) (string, error) {
			return strconv.Itoa(in * 2), nil
		},
		func(ctx *pipeContext) int {
			return ctx.Input
		},
		func(ctx *pipeContext, out string) {
			ctx.Output = out
		},
	)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = step(ctx)
	}
}

func BenchmarkPipeSeq(b *testing.B) {
	items := []int{1, 2, 3, 4, 5}
	ctx := context.Background()

	step := PipeSeq(
		slices.Values(items),
		func(ctx context.Context, item int) (int, error) {
			return item * 2, nil
		},
		func(ctx context.Context, item int) error {
			return nil
		},
	)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = step(ctx)
	}
}
