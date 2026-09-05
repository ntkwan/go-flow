package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/ntkwan/go-flow"
)

func primaryDatabase(ctx context.Context) error {
	return errors.New("primary db unreachable")
}

func readReplica(ctx context.Context) error {
	fmt.Println("read data from read replica successfully")
	return nil
}

func main() {
	step := flow.Step[context.Context](primaryDatabase).
		Fallback(readReplica)

	if err := step(context.Background()); err != nil {
		panic(err)
	}
}
