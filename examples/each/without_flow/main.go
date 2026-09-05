package main

import (
	"context"
	"fmt"
	"slices"
)

func processUser(ctx context.Context, name string) error {
	fmt.Printf("user processed: %s\n", name)
	return nil
}

func main() {
	ctx := context.Background()
	users := []string{"alice", "bob", "charlie"}
	seq := slices.Values(users)

	for user := range seq {
		if err := processUser(ctx, user); err != nil {
			panic(err)
		}
	}
}
