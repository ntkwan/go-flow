package leak_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/ntkwan/go-flow"
)

func TestLeakWorkflows(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()

	t.Run("Seq execution", func(t *testing.T) {
		s := flow.Seq(
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error { return nil },
		)
		_ = s(ctx)
	})

	t.Run("Go parallel execution", func(t *testing.T) {
		s := flow.Go(
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error { return nil },
		)
		_ = s(ctx)
	})

	t.Run("GoN bounded parallel execution", func(t *testing.T) {
		steps := make([]flow.Step[context.Context], 10)
		for i := range steps {
			steps[i] = func(ctx context.Context) error {
				time.Sleep(time.Millisecond)
				return nil
			}
		}
		s := flow.GoN(3, steps...)
		_ = s(ctx)
	})

	t.Run("DAG diamond execution", func(t *testing.T) {
		nodeA := flow.Node("A", func(ctx context.Context) error { return nil })
		nodeB := flow.Node("B", func(ctx context.Context) error { return nil }).After("A")
		nodeC := flow.Node("C", func(ctx context.Context) error { return nil }).After("A")
		nodeD := flow.Node("D", func(ctx context.Context) error { return nil }).After("B", "C")

		dag := flow.DAG(nodeA, nodeB, nodeC, nodeD)
		_ = dag(ctx)
	})

	t.Run("DAGN bounded diamond execution", func(t *testing.T) {
		nodeA := flow.Node("A", func(ctx context.Context) error { return nil })
		nodeB := flow.Node("B", func(ctx context.Context) error { return nil }).After("A")
		nodeC := flow.Node("C", func(ctx context.Context) error { return nil }).After("A")
		nodeD := flow.Node("D", func(ctx context.Context) error { return nil }).After("B", "C")

		dagn := flow.DAGN(2, nodeA, nodeB, nodeC, nodeD)
		_ = dagn(ctx)
	})

	t.Run("Race execution", func(t *testing.T) {
		stepFast := func(ctx context.Context) error { return nil }
		stepSlow := func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond * 50):
				return nil
			}
		}
		raceStep := flow.Race(stepSlow, stepFast)
		_ = raceStep(ctx)
	})

	t.Run("Timeout step", func(t *testing.T) {
		step := flow.Step[context.Context](func(ctx context.Context) error {
			time.Sleep(time.Millisecond)
			return nil
		}).Timeout(time.Second)
		_ = step(ctx)
	})

	t.Run("Retry step", func(t *testing.T) {
		attempts := 0
		step := flow.Step[context.Context](func(ctx context.Context) error {
			attempts++
			if attempts < 2 {
				return errors.New("temporary error")
			}
			return nil
		}).Retry(3, time.Millisecond)
		_ = step(ctx)
	})

	t.Run("Each and Chunk streams", func(t *testing.T) {
		items := []int{1, 2, 3, 4, 5}
		eachStep := flow.Each(slices.Values(items), func(ctx context.Context, item int) error {
			return nil
		})
		_ = eachStep(ctx)

		chunkStep := flow.Chunk(slices.Values(items), 2, func(ctx context.Context, batch []int) error {
			return nil
		})
		_ = chunkStep(ctx)
	})

	t.Run("Pipe step", func(t *testing.T) {
		type State struct {
			Count int
		}
		st := &State{Count: 10}
		pipeStep := flow.Pipe(
			func(ctx context.Context, in int) (int, error) {
				return in * 2, nil
			},
			func(ctx context.Context) int {
				return st.Count
			},
			func(ctx context.Context, out int) {
				st.Count = out
			},
		)
		_ = pipeStep(ctx)
	})

	t.Run("Dynamic execution", func(t *testing.T) {
		s := flow.Dynamic(func(ctx context.Context) flow.Step[context.Context] {
			return func(ctx context.Context) error { return nil }
		})
		_ = s(ctx)
	})
}
