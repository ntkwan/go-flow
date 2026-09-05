package flow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type customContext struct {
	context.Context
	CustomVal string
}

func TestStepExecution(t *testing.T) {
	called := false
	step := Step[context.Context](func(ctx context.Context) error {
		called = true
		return nil
	})

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !called {
		t.Fatal("expected step to be called")
	}
}

func TestStepCustomContext(t *testing.T) {
	var captured string
	step := Step[*customContext](func(ctx *customContext) error {
		captured = ctx.CustomVal
		return nil
	})

	ctx := &customContext{
		Context:   context.Background(),
		CustomVal: "custom",
	}

	if err := step(ctx); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if captured != "custom" {
		t.Fatalf("expected captured value 'custom', got %q", captured)
	}
}

func TestSeqEmpty(t *testing.T) {
	step := Seq[context.Context]()
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error for empty Seq, got %v", err)
	}
}

func TestSeqSuccess(t *testing.T) {
	var order []int
	var mu sync.Mutex

	appendOrder := func(n int) Step[context.Context] {
		return func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, n)
			return nil
		}
	}

	step := Seq(
		appendOrder(1),
		appendOrder(2),
		appendOrder(3),
	)

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("expected order [1 2 3], got %v", order)
	}
}

func TestSeqShortCircuit(t *testing.T) {
	var order []int
	errFail := errors.New("step 2 failed")

	step := Seq(
		func(ctx context.Context) error {
			order = append(order, 1)
			return nil
		},
		func(ctx context.Context) error {
			order = append(order, 2)
			return errFail
		},
		func(ctx context.Context) error {
			order = append(order, 3)
			return nil
		},
	)

	err := step(context.Background())
	if !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}

	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("expected order [1 2], got %v", order)
	}
}

func TestGoEmpty(t *testing.T) {
	step := Go[context.Context]()
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error for empty Go, got %v", err)
	}
}

func TestGoSuccess(t *testing.T) {
	var count atomic.Int32

	steps := make([]Step[context.Context], 10)
	for i := range steps {
		steps[i] = func(ctx context.Context) error {
			count.Add(1)
			return nil
		}
	}

	step := Go(steps...)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if count.Load() != 10 {
		t.Fatalf("expected count 10, got %d", count.Load())
	}
}

func TestGoError(t *testing.T) {
	err1 := errors.New("err 1")
	err2 := errors.New("err 2")

	step := Go(
		func(ctx context.Context) error {
			return err1
		},
		func(ctx context.Context) error {
			return nil
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
		t.Fatalf("expected joined errors containing err1 and err2, got %v", err)
	}
}

func TestGoConcurrency(t *testing.T) {
	start := time.Now()
	step := Go(
		func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		},
		func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	)

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	elapsed := time.Since(start)
	if elapsed >= 90*time.Millisecond {
		t.Fatalf("expected concurrent execution under 90ms, took %v", elapsed)
	}
}

func TestStepMethodExec(t *testing.T) {
	var step Step[context.Context]
	if err := step.Exec(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step Exec, got %v", err)
	}

	called := false
	step = func(ctx context.Context) error {
		called = true
		return nil
	}
	if err := step.Exec(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !called {
		t.Fatal("expected step to be executed")
	}
}

func TestStepMethodThen(t *testing.T) {
	var calls []string
	s1 := Step[context.Context](func(ctx context.Context) error {
		calls = append(calls, "s1")
		return nil
	})
	s2 := Step[context.Context](func(ctx context.Context) error {
		calls = append(calls, "s2")
		return nil
	})
	s3 := Step[context.Context](func(ctx context.Context) error {
		calls = append(calls, "s3")
		return nil
	})

	chained := s1.Then(s2, s3)
	if err := chained(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(calls) != 3 || calls[0] != "s1" || calls[1] != "s2" || calls[2] != "s3" {
		t.Fatalf("expected [s1 s2 s3], got %v", calls)
	}

	calls = nil
	var nilStep Step[context.Context]
	chainedNil := nilStep.Then(s1, s2)
	if err := chainedNil(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(calls) != 2 || calls[0] != "s1" || calls[1] != "s2" {
		t.Fatalf("expected [s1 s2], got %v", calls)
	}
}

func TestStepMethodGo(t *testing.T) {
	var count atomic.Int32
	s1 := Step[context.Context](func(ctx context.Context) error {
		count.Add(1)
		return nil
	})
	s2 := Step[context.Context](func(ctx context.Context) error {
		count.Add(1)
		return nil
	})
	s3 := Step[context.Context](func(ctx context.Context) error {
		count.Add(1)
		return nil
	})

	concurrent := s1.Go(s2, s3)
	if err := concurrent(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if count.Load() != 3 {
		t.Fatalf("expected 3 executions, got %d", count.Load())
	}

	count.Store(0)
	var nilStep Step[context.Context]
	concurrentNil := nilStep.Go(s1, s2)
	if err := concurrentNil(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if count.Load() != 2 {
		t.Fatalf("expected 2 executions, got %d", count.Load())
	}
}

func TestStepMethodGoN(t *testing.T) {
	var count atomic.Int32
	s1 := Step[context.Context](func(ctx context.Context) error {
		count.Add(1)
		return nil
	})
	s2 := Step[context.Context](func(ctx context.Context) error {
		count.Add(1)
		return nil
	})
	s3 := Step[context.Context](func(ctx context.Context) error {
		count.Add(1)
		return nil
	})

	concurrent := s1.GoN(2, s2, s3)
	if err := concurrent(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if count.Load() != 3 {
		t.Fatalf("expected 3 executions, got %d", count.Load())
	}

	count.Store(0)
	var nilStep Step[context.Context]
	concurrentNil := nilStep.GoN(1, s1, s2)
	if err := concurrentNil(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if count.Load() != 2 {
		t.Fatalf("expected 2 executions, got %d", count.Load())
	}
}

func TestStepMethodRace(t *testing.T) {
	s1 := Step[context.Context](func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	s2 := Step[context.Context](func(ctx context.Context) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	s3 := Step[context.Context](func(ctx context.Context) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	racing := s1.Race(s2, s3)
	if err := racing(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	var nilStep Step[context.Context]
	racingNil := nilStep.Race(s1, s2)
	if err := racingNil(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestStepMethodOnce(t *testing.T) {
	var count atomic.Int32
	s := Step[context.Context](func(ctx context.Context) error {
		count.Add(1)
		return nil
	}).Once()

	for range 3 {
		_ = s(context.Background())
	}
	if count.Load() != 1 {
		t.Fatalf("expected 1 execution, got %d", count.Load())
	}
}

func TestStepMethodTimeout(t *testing.T) {
	var nilStep Step[context.Context]
	if err := nilStep.Timeout(10 * time.Millisecond)(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step, got %v", err)
	}

	fastStep := Step[context.Context](func(ctx context.Context) error {
		return nil
	}).Timeout(50 * time.Millisecond)

	if err := fastStep(context.Background()); err != nil {
		t.Fatalf("expected nil error for fast step, got %v", err)
	}

	slowStep := Step[context.Context](func(ctx context.Context) error {
		select {
		case <-time.After(100 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}).Timeout(10 * time.Millisecond)

	err := slowStep(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestStepMethodRetry(t *testing.T) {
	var nilStep Step[context.Context]
	if err := nilStep.Retry(3, 0)(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step, got %v", err)
	}

	errTarget := errors.New("err target")
	var zeroRuns atomic.Int32
	zeroAttemptsStep := Step[context.Context](func(ctx context.Context) error {
		zeroRuns.Add(1)
		return errTarget
	}).Retry(0, 0)
	if err := zeroAttemptsStep(context.Background()); !errors.Is(err, errTarget) {
		t.Fatalf("expected errTarget for zero attempts, got %v", err)
	}
	if zeroRuns.Load() != 1 {
		t.Fatalf("expected exactly 1 attempt for 0 attempts, got %d", zeroRuns.Load())
	}

	var negRuns atomic.Int32
	negAttemptsStep := Step[context.Context](func(ctx context.Context) error {
		negRuns.Add(1)
		return errTarget
	}).Retry(-5, 0)
	if err := negAttemptsStep(context.Background()); !errors.Is(err, errTarget) {
		t.Fatalf("expected errTarget for negative attempts, got %v", err)
	}
	if negRuns.Load() != 1 {
		t.Fatalf("expected exactly 1 attempt for negative attempts, got %d", negRuns.Load())
	}

	var attempts atomic.Int32
	failTwoTimes := Step[context.Context](func(ctx context.Context) error {
		n := attempts.Add(1)
		if n < 3 {
			return errors.New("temporary error")
		}
		return nil
	}).Retry(3, 5*time.Millisecond)

	if err := failTwoTimes(context.Background()); err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}

	errAlways := errors.New("permanent failure")
	alwaysFail := Step[context.Context](func(ctx context.Context) error {
		return errAlways
	}).Retry(2, 0)

	if err := alwaysFail(context.Background()); !errors.Is(err, errAlways) {
		t.Fatalf("expected %v, got %v", errAlways, err)
	}

	start := time.Now()
	var failAttempts atomic.Int32
	delayMeasured := Step[context.Context](func(ctx context.Context) error {
		failAttempts.Add(1)
		return errors.New("fail")
	}).Retry(3, 10*time.Millisecond)

	_ = delayMeasured(context.Background())
	elapsed := time.Since(start)
	if failAttempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", failAttempts.Load())
	}
	if elapsed < 18*time.Millisecond {
		t.Fatalf("expected at least 2 delay periods (~20ms), took %v", elapsed)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceledRetry := Step[context.Context](func(c context.Context) error {
		return errors.New("err")
	}).Retry(3, 100*time.Millisecond)

	if err := canceledRetry(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	canceledDuringDelay := Step[context.Context](func(c context.Context) error {
		cancel2()
		return errors.New("err")
	}).Retry(3, 100*time.Millisecond)

	if err := canceledDuringDelay(ctx2); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled during delay, got %v", err)
	}
}

func TestStepMethodFallback(t *testing.T) {
	var nilStep Step[context.Context]
	fallbackCalled := false
	fb := Step[context.Context](func(ctx context.Context) error {
		fallbackCalled = true
		return nil
	})

	if err := nilStep.Fallback(fb)(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !fallbackCalled {
		t.Fatal("expected fallback to be called on nil step")
	}

	if err := nilStep.Fallback(nil)(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	successStep := Step[context.Context](func(ctx context.Context) error {
		return nil
	}).Fallback(fb)

	fallbackCalled = false
	if err := successStep(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if fallbackCalled {
		t.Fatal("fallback should not be called when primary succeeds")
	}

	failStep := Step[context.Context](func(ctx context.Context) error {
		return errors.New("primary fail")
	}).Fallback(fb)

	if err := failStep(context.Background()); err != nil {
		t.Fatalf("expected fallback to handle error, got %v", err)
	}
	if !fallbackCalled {
		t.Fatal("expected fallback to be called on primary failure")
	}

	errPrimary := errors.New("primary fail")
	failStepNoFallback := Step[context.Context](func(ctx context.Context) error {
		return errPrimary
	}).Fallback(nil)

	if err := failStepNoFallback(context.Background()); !errors.Is(err, errPrimary) {
		t.Fatalf("expected primary error, got %v", err)
	}
}

func TestStepMethodCatch(t *testing.T) {
	var nilStep Step[context.Context]
	if err := nilStep.Catch(nil)(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step, got %v", err)
	}

	successStep := Step[context.Context](func(ctx context.Context) error {
		return nil
	}).Catch(func(ctx context.Context, err error) error {
		return errors.New("should not happen")
	})
	if err := successStep(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	errOrig := errors.New("original error")
	errTransformed := errors.New("transformed error")

	failStep := Step[context.Context](func(ctx context.Context) error {
		return errOrig
	}).Catch(func(ctx context.Context, err error) error {
		if errors.Is(err, errOrig) {
			return errTransformed
		}
		return err
	})

	if err := failStep(context.Background()); !errors.Is(err, errTransformed) {
		t.Fatalf("expected %v, got %v", errTransformed, err)
	}

	failStepNilHandler := Step[context.Context](func(ctx context.Context) error {
		return errOrig
	}).Catch(nil)
	if err := failStepNilHandler(context.Background()); !errors.Is(err, errOrig) {
		t.Fatalf("expected %v, got %v", errOrig, err)
	}
}

func TestStepMethodRecover(t *testing.T) {
	var nilStep Step[context.Context]
	if err := nilStep.Recover()(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step, got %v", err)
	}

	panicErrStep := Step[context.Context](func(ctx context.Context) error {
		panic(errors.New("panic error"))
	}).Recover()

	err := panicErrStep(context.Background())
	if err == nil {
		t.Fatal("expected error after panic recovery, got nil")
	}

	panicStrStep := Step[context.Context](func(ctx context.Context) error {
		panic("panic string")
	}).Recover()

	if err := panicStrStep(context.Background()); err == nil {
		t.Fatal("expected error after string panic recovery, got nil")
	}

	normalStep := Step[context.Context](func(ctx context.Context) error {
		return nil
	}).Recover()
	if err := normalStep(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestStepMethodWhen(t *testing.T) {
	var nilStep Step[context.Context]
	if err := nilStep.When(nil)(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step, got %v", err)
	}

	ran := false
	step := Step[context.Context](func(ctx context.Context) error {
		ran = true
		return nil
	})

	if err := step.When(func(ctx context.Context) bool { return false })(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ran {
		t.Fatal("step should not run when predicate is false")
	}

	if err := step.When(func(ctx context.Context) bool { return true })(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !ran {
		t.Fatal("step should run when predicate is true")
	}
}

func TestStepMethodUnless(t *testing.T) {
	var nilStep Step[context.Context]
	if err := nilStep.Unless(nil)(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step, got %v", err)
	}

	ran := false
	step := Step[context.Context](func(ctx context.Context) error {
		ran = true
		return nil
	})

	if err := step.Unless(func(ctx context.Context) bool { return true })(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ran {
		t.Fatal("step should not run when predicate is true in Unless")
	}

	if err := step.Unless(func(ctx context.Context) bool { return false })(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !ran {
		t.Fatal("step should run when predicate is false in Unless")
	}
}

func TestStepMethodWrap(t *testing.T) {
	var nilStep Step[context.Context]
	if err := nilStep.Wrap()(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step, got %v", err)
	}

	var executionOrder []string
	mw1 := func(next Step[context.Context]) Step[context.Context] {
		return func(ctx context.Context) error {
			executionOrder = append(executionOrder, "mw1_before")
			err := next(ctx)
			executionOrder = append(executionOrder, "mw1_after")
			return err
		}
	}
	mw2 := func(next Step[context.Context]) Step[context.Context] {
		return func(ctx context.Context) error {
			executionOrder = append(executionOrder, "mw2_before")
			err := next(ctx)
			executionOrder = append(executionOrder, "mw2_after")
			return err
		}
	}

	coreStep := Step[context.Context](func(ctx context.Context) error {
		executionOrder = append(executionOrder, "core")
		return nil
	})

	wrapped := coreStep.Wrap(mw1, nil, mw2)
	if err := wrapped(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	expected := []string{"mw1_before", "mw2_before", "core", "mw2_after", "mw1_after"}
	if len(executionOrder) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, executionOrder)
	}
	for i, v := range expected {
		if executionOrder[i] != v {
			t.Fatalf("at index %d: expected %s, got %s", i, v, executionOrder[i])
		}
	}
}

func BenchmarkSeq(b *testing.B) {
	step := Seq(
		func(ctx context.Context) error { return nil },
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

func BenchmarkGo(b *testing.B) {
	step := Go(
		func(ctx context.Context) error { return nil },
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

func TestBranchTrue(t *testing.T) {
	ifRan := false
	elseRan := false
	step := Branch(
		func(ctx context.Context) bool { return true },
		func(ctx context.Context) error {
			ifRan = true
			return nil
		},
		func(ctx context.Context) error {
			elseRan = true
			return nil
		},
	)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !ifRan || elseRan {
		t.Fatalf("expected only ifBranch to run, ifRan=%v elseRan=%v", ifRan, elseRan)
	}
}

func TestBranchFalse(t *testing.T) {
	ifRan := false
	elseRan := false
	step := Branch(
		func(ctx context.Context) bool { return false },
		func(ctx context.Context) error {
			ifRan = true
			return nil
		},
		func(ctx context.Context) error {
			elseRan = true
			return nil
		},
	)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ifRan || !elseRan {
		t.Fatalf("expected only elseBranch to run, ifRan=%v elseRan=%v", ifRan, elseRan)
	}
}

func TestBranchNilBranches(t *testing.T) {
	stepTrue := Branch[context.Context](func(ctx context.Context) bool { return true }, nil, nil)
	if err := stepTrue(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	stepFalse := Branch[context.Context](func(ctx context.Context) bool { return false }, nil, nil)
	if err := stepFalse(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	stepNilCond := Branch[context.Context](nil, nil, nil)
	if err := stepNilCond(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestStepBranch(t *testing.T) {
	var trace []string
	step := Step[context.Context](func(ctx context.Context) error {
		trace = append(trace, "init")
		return nil
	}).Branch(
		func(ctx context.Context) bool { return true },
		func(ctx context.Context) error {
			trace = append(trace, "branch_true")
			return nil
		},
		func(ctx context.Context) error {
			trace = append(trace, "branch_false")
			return nil
		},
	)

	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(trace) != 2 || trace[0] != "init" || trace[1] != "branch_true" {
		t.Fatalf("unexpected trace: %v", trace)
	}
}

func BenchmarkBranch(b *testing.B) {
	step := Branch(
		func(ctx context.Context) bool { return true },
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
