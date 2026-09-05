package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var attempts int

func unstableNetworkCall(ctx context.Context) error {
	attempts++
	fmt.Printf("attempt %d\n", attempts)
	if attempts < 3 {
		return errors.New("network error")
	}
	fmt.Println("success!")
	return nil
}

func main() {
	ctx := context.Background()
	maxAttempts := 3
	delay := 10 * time.Millisecond

	var err error
	for i := 0; i < maxAttempts; i++ {
		if ctx.Err() != nil {
			panic(ctx.Err())
		}

		err = unstableNetworkCall(ctx)
		if err == nil {
			break
		}

		if i < maxAttempts-1 && delay > 0 {
			select {
			case <-ctx.Done():
				panic(ctx.Err())
			case <-time.After(delay):
			}
		}
	}

	if err != nil {
		panic(err)
	}
}
