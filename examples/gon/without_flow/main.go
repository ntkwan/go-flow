package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

func processItem(ctx context.Context, id int) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Printf("item %d processed\n", id)
	return nil
}

func main() {
	total := 6
	limit := 2

	errs := make([]error, total)
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup

	ctx := context.Background()
	for i := 1; i <= total; i++ {
		idx := i - 1
		id := i
		sem <- struct{}{}
		wg.Add(1)
		go func(index int, itemID int) {
			defer func() {
				<-sem
				wg.Done()
			}()
			errs[index] = processItem(ctx, itemID)
		}(idx, id)
	}

	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		panic(err)
	}
}
