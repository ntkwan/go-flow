package cancellation_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntkwan/go-flow"
)

func TestCancellationMatrixPreCanceled(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	var executed atomic.Bool

	stepTrack := func(ctx context.Context) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		executed.Store(true)
		return nil
	}

	tests := []struct {
		name string
		step flow.Step[context.Context]
	}{
		{
			name: "Seq with pre-canceled check step",
			step: flow.Seq(stepTrack),
		},
		{
			name: "Go with pre-canceled check step",
			step: flow.Go(stepTrack),
		},
		{
			name: "GoN with pre-canceled check step",
			step: flow.GoN(2, stepTrack),
		},
		{
			name: "DAG with pre-canceled context",
			step: flow.DAG(flow.Node("A", stepTrack)),
		},
		{
			name: "DAGN with pre-canceled context",
			step: flow.DAGN(1, flow.Node("A", stepTrack)),
		},
		{
			name: "Race with pre-canceled context",
			step: flow.Race(stepTrack),
		},
		{
			name: "Retry with pre-canceled context",
			step: flow.Step[context.Context](stepTrack).Retry(3, time.Millisecond),
		},
		{
			name: "Timeout with pre-canceled context",
			step: flow.Step[context.Context](stepTrack).Timeout(time.Second),
		},
		{
			name: "Each with pre-canceled context",
			step: flow.Each(slices.Values([]int{1, 2}), func(ctx context.Context, item int) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				executed.Store(true)
				return nil
			}),
		},
		{
			name: "Chunk with pre-canceled context",
			step: flow.Chunk(slices.Values([]int{1, 2}), 1, func(ctx context.Context, batch []int) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				executed.Store(true)
				return nil
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executed.Store(false)
			err := tc.step(canceledCtx)
			if err == nil {
				t.Fatalf("%s: expected context cancellation error, got nil", tc.name)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s: expected context.Canceled error, got: %v", tc.name, err)
			}
		})
	}
}

func TestCancellationMatrixMidFlight(t *testing.T) {
	t.Run("DAG mid-flight cancellation aborts downstream", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var bExecuted atomic.Bool

		nodeA := flow.Node("A", func(c context.Context) error {
			cancel()
			return nil
		})
		nodeB := flow.Node("B", func(c context.Context) error {
			bExecuted.Store(true)
			return nil
		}).After("A")

		err := flow.DAG(nodeA, nodeB)(ctx)
		if err == nil {
			t.Fatal("expected error on canceled DAG")
		}
		if bExecuted.Load() {
			t.Fatal("node B should not have executed after cancellation")
		}
	})

	t.Run("DAGN mid-flight cancellation aborts blocked tasks", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var bExecuted atomic.Bool

		nodeA := flow.Node("A", func(c context.Context) error {
			cancel()
			return nil
		})
		nodeB := flow.Node("B", func(c context.Context) error {
			bExecuted.Store(true)
			return nil
		}).After("A")

		err := flow.DAGN(1, nodeA, nodeB)(ctx)
		if err == nil {
			t.Fatal("expected error on canceled DAGN")
		}
		if bExecuted.Load() {
			t.Fatal("node B should not have executed after DAGN cancellation")
		}
	})

	t.Run("Race cancels remaining siblings on success", func(t *testing.T) {
		var siblingCancelled atomic.Bool

		stepWinner := func(ctx context.Context) error {
			return nil
		}
		stepLoser := func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				siblingCancelled.Store(true)
				return ctx.Err()
			case <-time.After(time.Millisecond * 200):
				return nil
			}
		}

		err := flow.Race(stepWinner, stepLoser)(context.Background())
		if err != nil {
			t.Fatalf("expected race success, got %v", err)
		}

		time.Sleep(time.Millisecond * 50)
		if !siblingCancelled.Load() {
			t.Fatal("loser step should receive cancellation signal when winner finishes")
		}
	})
}
