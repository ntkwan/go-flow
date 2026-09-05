package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
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
	step := flow.Step[context.Context](unstableNetworkCall).
		Retry(3, 10*time.Millisecond)

	if err := step(context.Background()); err != nil {
		panic(err)
	}
}
