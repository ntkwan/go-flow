package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

func queryPrimaryServer(ctx context.Context) error {
	time.Sleep(100 * time.Millisecond)
	fmt.Println("primary server responded")
	return nil
}

func queryMirrorServer(ctx context.Context) error {
	time.Sleep(20 * time.Millisecond)
	fmt.Println("mirror server responded")
	return nil
}

func main() {
	workflow := flow.Race(
		queryPrimaryServer,
		queryMirrorServer,
	)

	if err := workflow(context.Background()); err != nil {
		panic(err)
	}
}
