package main

import (
	"context"
	"fmt"
	"sync"
)

var (
	once    sync.Once
	initErr error
)

func initializeConnection(ctx context.Context) error {
	fmt.Println("expensive connection pool initialized")
	return nil
}

func queryData(ctx context.Context) error {
	fmt.Println("query executed")
	return nil
}

func initOnce(ctx context.Context) error {
	once.Do(func() {
		initErr = initializeConnection(ctx)
	})
	return initErr
}

func main() {
	ctx := context.Background()

	if err := initOnce(ctx); err == nil {
		_ = queryData(ctx)
	}

	if err := initOnce(ctx); err == nil {
		_ = queryData(ctx)
	}
}
