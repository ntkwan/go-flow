package main

import (
	"context"
	"errors"
	"fmt"
	"time"
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
	tasks := []func(context.Context) error{
		queryPrimaryServer,
		queryMirrorServer,
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	resultCh := make(chan error, len(tasks))
	for _, task := range tasks {
		t := task
		go func() {
			resultCh <- t(ctx)
		}()
	}

	var errs []error
	for range tasks {
		select {
		case <-ctx.Done():
			panic(context.Cause(ctx))
		case err := <-resultCh:
			if err == nil {
				cancel(nil)
				return
			}
			errs = append(errs, err)
		}
	}

	if err := errors.Join(errs...); err != nil {
		panic(err)
	}
}
