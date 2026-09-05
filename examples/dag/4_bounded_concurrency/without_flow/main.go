package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

func task1(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Task 1 completed")
	return nil
}

func task2(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Task 2 completed")
	return nil
}

func task3(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Task 3 completed")
	return nil
}

func aggregateTask(ctx context.Context) error {
	fmt.Println("Aggregation task completed")
	return nil
}

func main() {
	ctx := context.Background()

	sem := make(chan struct{}, 2)
	var err1, err2, err3, errAgg error

	t1Done := make(chan struct{})
	t2Done := make(chan struct{})
	t3Done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer func() {
			close(t1Done)
			wg.Done()
		}()
		sem <- struct{}{}
		defer func() { <-sem }()
		err1 = task1(ctx)
	}()

	go func() {
		defer func() {
			close(t2Done)
			wg.Done()
		}()
		sem <- struct{}{}
		defer func() { <-sem }()
		err2 = task2(ctx)
	}()

	go func() {
		defer func() {
			close(t3Done)
			wg.Done()
		}()
		sem <- struct{}{}
		defer func() { <-sem }()
		err3 = task3(ctx)
	}()

	go func() {
		defer wg.Done()
		<-t1Done
		<-t2Done
		<-t3Done
		if err1 != nil || err2 != nil || err3 != nil {
			return
		}
		sem <- struct{}{}
		defer func() { <-sem }()
		errAgg = aggregateTask(ctx)
	}()

	wg.Wait()
	if err := errors.Join(err1, err2, err3, errAgg); err != nil {
		panic(err)
	}
}
