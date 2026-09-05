package main

import (
	"context"
	"fmt"
	"time"
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
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- longRunningTask(ctx)
	}()

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-done:
	}

	fmt.Printf("result: %v\n", err)
}
