package main

import (
	"context"
	"fmt"
	"time"
)

func doBusinessLogic(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("business logic executed")
	return nil
}

func main() {
	ctx := context.Background()

	start := time.Now()
	fmt.Println("before step")
	err := doBusinessLogic(ctx)
	fmt.Printf("after step: elapsed %v\n", time.Since(start))

	if err != nil {
		panic(err)
	}
}
