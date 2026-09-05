package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

func fetchUserProfile(ctx context.Context) error {
	time.Sleep(20 * time.Millisecond)
	fmt.Println("profile fetched")
	return nil
}

func fetchUserOrders(ctx context.Context) error {
	time.Sleep(30 * time.Millisecond)
	fmt.Println("orders fetched")
	return nil
}

func fetchUserPreferences(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("preferences fetched")
	return nil
}

func main() {
	workflow := flow.Go(
		fetchUserProfile,
		fetchUserOrders,
		fetchUserPreferences,
	)

	if err := workflow(context.Background()); err != nil {
		panic(err)
	}
}
