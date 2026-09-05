package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

func longRunningTask(ctx context.Context) error {
	select {
	case <-time.After(200 * time.Millisecond):
		fmt.Println("task finished")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func main() {
	step := flow.Step[context.Context](longRunningTask).
		Timeout(50 * time.Millisecond)

	err := step(context.Background())
	fmt.Printf("result: %v\n", err)
}
