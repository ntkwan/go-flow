package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

func loggingMiddleware(next flow.Step[context.Context]) flow.Step[context.Context] {
	return func(ctx context.Context) error {
		start := time.Now()
		fmt.Println("before step")
		err := next(ctx)
		fmt.Printf("after step: elapsed %v\n", time.Since(start))
		return err
	}
}

func doBusinessLogic(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("business logic executed")
	return nil
}

func main() {
	step := flow.Step[context.Context](doBusinessLogic).
		Wrap(loggingMiddleware)

	if err := step(context.Background()); err != nil {
		panic(err)
	}
}
