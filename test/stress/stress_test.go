package stress_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntkwan/go-flow"
)

func TestStressDiamondDAG(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		var a, b, c, d atomic.Int32

		nodeA := flow.Node("A", func(ctx context.Context) error {
			a.Add(1)
			return nil
		})
		nodeB := flow.Node("B", func(ctx context.Context) error {
			if a.Load() != 1 {
				t.Errorf("B executed before A")
			}
			b.Add(1)
			return nil
		}).After("A")
		nodeC := flow.Node("C", func(ctx context.Context) error {
			if a.Load() != 1 {
				t.Errorf("C executed before A")
			}
			c.Add(1)
			return nil
		}).After("A")
		nodeD := flow.Node("D", func(ctx context.Context) error {
			if b.Load() != 1 || c.Load() != 1 {
				t.Errorf("D executed before B or C")
			}
			d.Add(1)
			return nil
		}).After("B", "C")

		err := flow.DAG(nodeA, nodeB, nodeC, nodeD)(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if a.Load() != 1 || b.Load() != 1 || c.Load() != 1 || d.Load() != 1 {
			t.Fatalf("nodes did not execute exactly once")
		}
	}
}

func TestStressConcurrentGoN(t *testing.T) {
	const totalSteps = 100
	const concurrencyLimit = 5

	var active atomic.Int32
	var maxActive atomic.Int32
	var completed atomic.Int32

	steps := make([]flow.Step[context.Context], totalSteps)
	for i := 0; i < totalSteps; i++ {
		steps[i] = func(ctx context.Context) error {
			cur := active.Add(1)
			for {
				maxVal := maxActive.Load()
				if cur <= maxVal || maxActive.CompareAndSwap(maxVal, cur) {
					break
				}
			}
			time.Sleep(time.Microsecond * 100)
			active.Add(-1)
			completed.Add(1)
			return nil
		}
	}

	err := flow.GoN(concurrencyLimit, steps...)(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if completed.Load() != totalSteps {
		t.Fatalf("expected %d completed, got %d", totalSteps, completed.Load())
	}
	if maxActive.Load() > concurrencyLimit {
		t.Fatalf("max concurrency %d exceeded limit %d", maxActive.Load(), concurrencyLimit)
	}
}

func TestStressRaceCombinator(t *testing.T) {
	for i := 0; i < 30; i++ {
		stepFast := func(ctx context.Context) error {
			return nil
		}
		stepSlow := func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond * 50):
				return nil
			}
		}
		stepErr := func(ctx context.Context) error {
			return errors.New("fail")
		}

		err := flow.Race(stepSlow, stepFast, stepErr)(context.Background())
		if err != nil {
			t.Fatalf("iteration %d: expected race to succeed with fastest step, got: %v", i, err)
		}
	}
}

func TestStressParallelWorkflowExecution(t *testing.T) {
	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			var counter atomic.Int32
			s1 := func(ctx context.Context) error {
				counter.Add(1)
				return nil
			}
			s2 := func(ctx context.Context) error {
				counter.Add(2)
				return nil
			}
			s3 := func(ctx context.Context) error {
				counter.Add(3)
				return nil
			}
			wf := flow.Seq(s1, flow.Go(s2, s3))
			if err := wf(context.Background()); err != nil {
				t.Errorf("workflow execution failed: %v", err)
			}
			if counter.Load() != 6 {
				t.Errorf("expected counter 6, got %d", counter.Load())
			}
		}()
	}

	wg.Wait()
}
