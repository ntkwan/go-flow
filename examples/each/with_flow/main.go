package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/ntkwan/go-flow"
)

func processUser(ctx context.Context, name string) error {
	fmt.Printf("user processed: %s\n", name)
	return nil
}

func main() {
	users := []string{"alice", "bob", "charlie"}
	seq := slices.Values(users)

	step := flow.Each(seq, processUser)
	if err := step(context.Background()); err != nil {
		panic(err)
	}
}
