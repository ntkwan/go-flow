package flow_test

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ntkwan/go-flow"
)

func ExampleSeq() {
	step1 := func(ctx context.Context) error {
		fmt.Println("Step 1")
		return nil
	}
	step2 := func(ctx context.Context) error {
		fmt.Println("Step 2")
		return nil
	}

	pipeline := flow.Seq(step1, step2)
	_ = pipeline(context.Background())
}

func ExampleGo() {
	stepA := func(ctx context.Context) error {
		fmt.Println("Task A")
		return nil
	}
	stepB := func(ctx context.Context) error {
		fmt.Println("Task B")
		return nil
	}

	parallel := flow.Go(stepA, stepB)
	_ = parallel(context.Background())
}

func ExampleGoN() {
	tasks := make([]flow.Step[context.Context], 5)
	for i := range tasks {
		idx := i
		tasks[i] = func(ctx context.Context) error {
			fmt.Printf("Worker task %d\n", idx)
			return nil
		}
	}

	bounded := flow.GoN(2, tasks...)
	_ = bounded(context.Background())
}

func ExampleRace() {
	server1 := func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		fmt.Println("Server 1 responded")
		return nil
	}
	server2 := func(ctx context.Context) error {
		fmt.Println("Server 2 responded fastest")
		return nil
	}

	fastest := flow.Race(server1, server2)
	_ = fastest(context.Background())
}

func ExampleBranch() {
	type RequestContext struct {
		context.Context
		IsAdmin bool
	}

	adminStep := func(ctx *RequestContext) error {
		fmt.Println("Admin flow")
		return nil
	}
	userStep := func(ctx *RequestContext) error {
		fmt.Println("User flow")
		return nil
	}

	route := flow.Branch(
		func(ctx *RequestContext) bool { return ctx.IsAdmin },
		adminStep,
		userStep,
	)

	ctx := &RequestContext{Context: context.Background(), IsAdmin: true}
	_ = route(ctx)
}

func ExamplePipe() {
	type State struct {
		Count int
	}
	st := &State{Count: 5}

	doubleCount := flow.Pipe(
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

	_ = doubleCount(context.Background())
	fmt.Printf("Result: %d\n", st.Count)
}

func ExamplePipeSeq() {
	numbers := slices.Values([]int{1, 2, 3})
	stream := flow.PipeSeq(
		numbers,
		func(ctx context.Context, in int) (string, error) {
			return fmt.Sprintf("val-%d", in*10), nil
		},
		func(ctx context.Context, out string) error {
			fmt.Println(out)
			return nil
		},
	)

	_ = stream(context.Background())
}

func ExampleEach() {
	items := slices.Values([]string{"apple", "banana", "cherry"})
	process := flow.Each(items, func(ctx context.Context, item string) error {
		fmt.Println("Processing:", item)
		return nil
	})

	_ = process(context.Background())
}

func ExampleChunk() {
	numbers := []int{1, 2, 3, 4, 5, 6, 7}
	batchProcess := flow.Chunk(slices.Values(numbers), 3, func(ctx context.Context, batch []int) error {
		fmt.Printf("Batch of %d items: %v\n", len(batch), batch)
		return nil
	})

	_ = batchProcess(context.Background())
}

func ExampleOnce() {
	initDB := flow.Once(func(ctx context.Context) error {
		fmt.Println("Database initialized once")
		return nil
	})

	step1 := flow.Seq(initDB, func(ctx context.Context) error { return nil })
	step2 := flow.Seq(initDB, func(ctx context.Context) error { return nil })

	pipeline := flow.Go(step1, step2)
	_ = pipeline(context.Background())
}

func ExampleStep_Retry() {
	attempts := 0
	flaky := flow.Step[context.Context](func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("temporary failure %d", attempts)
		}
		fmt.Println("Succeeded on attempt 3")
		return nil
	})

	resilient := flaky.Retry(3, 10*time.Millisecond)
	_ = resilient.Exec(context.Background())
}

func ExampleStep_Timeout() {
	slow := flow.Step[context.Context](func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	timed := slow.Timeout(10 * time.Millisecond)
	err := timed.Exec(context.Background())
	fmt.Println("Error:", err != nil)
}

func ExampleStep_Fallback() {
	failingPrimary := flow.Step[context.Context](func(ctx context.Context) error {
		return fmt.Errorf("primary service down")
	})
	backupService := flow.Step[context.Context](func(ctx context.Context) error {
		fmt.Println("Backup service handled request")
		return nil
	})

	protected := failingPrimary.Fallback(backupService)
	_ = protected.Exec(context.Background())
}

func ExampleStep_When() {
	type OrderContext struct {
		context.Context
		IsVIP bool
	}

	vipGift := flow.Step[*OrderContext](func(ctx *OrderContext) error {
		fmt.Println("VIP gift applied")
		return nil
	}).When(func(ctx *OrderContext) bool {
		return ctx.IsVIP
	})

	ctx := &OrderContext{Context: context.Background(), IsVIP: true}
	_ = vipGift.Exec(ctx)
}

func ExampleStep_Wrap() {
	base := flow.Step[context.Context](func(ctx context.Context) error {
		fmt.Println("Core logic")
		return nil
	})

	loggingMiddleware := func(next flow.Step[context.Context]) flow.Step[context.Context] {
		return func(ctx context.Context) error {
			fmt.Println("Before step")
			err := next(ctx)
			fmt.Println("After step")
			return err
		}
	}

	wrapped := base.Wrap(loggingMiddleware)
	_ = wrapped.Exec(context.Background())
}

func ExampleRecovery() {
	riskyStep := flow.Step[context.Context](func(ctx context.Context) error {
		panic("database connection lost")
	})

	guarded := riskyStep.Wrap(flow.Recovery[context.Context]())
	err := guarded.Exec(context.Background())
	fmt.Println("Recovered from panic:", err != nil)
}
