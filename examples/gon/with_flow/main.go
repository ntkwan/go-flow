package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

func processItem(ctx context.Context, id int) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Printf("item %d processed\n", id)
	return nil
}

func main() {
	var steps []flow.Step[context.Context]
	for i := 1; i <= 6; i++ {
		id := i
		steps = append(steps, func(ctx context.Context) error {
			return processItem(ctx, id)
		})
	}

	workflow := flow.GoN(2, steps...)
	if err := workflow(context.Background()); err != nil {
		panic(err)
	}
}
