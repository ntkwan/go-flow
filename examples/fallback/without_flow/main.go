package main

import (
	"context"
	"errors"
	"fmt"
)

func primaryDatabase(ctx context.Context) error {
	return errors.New("primary db unreachable")
}

func readReplica(ctx context.Context) error {
	fmt.Println("read data from read replica successfully")
	return nil
}

func main() {
	ctx := context.Background()

	err := primaryDatabase(ctx)
	if err != nil {
		err = readReplica(ctx)
	}

	if err != nil {
		panic(err)
	}
}
