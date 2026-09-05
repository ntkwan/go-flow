package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
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
	tasks := []func(context.Context) error{
		fetchUserProfile,
		fetchUserOrders,
		fetchUserPreferences,
	}

	errs := make([]error, len(tasks))
	var wg sync.WaitGroup
	wg.Add(len(tasks))

	ctx := context.Background()
	for i, task := range tasks {
		go func(idx int, t func(context.Context) error) {
			defer wg.Done()
			errs[idx] = t(ctx)
		}(i, task)
	}

	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		panic(err)
	}
}
