package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

func fetchUser(ctx context.Context) error {
	time.Sleep(20 * time.Millisecond)
	fmt.Println("user fetched")
	return nil
}

func fetchCart(ctx context.Context) error {
	time.Sleep(20 * time.Millisecond)
	fmt.Println("cart fetched")
	return nil
}

func processPayment(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("payment processed")
	return nil
}

func sendReceipt(ctx context.Context) error {
	fmt.Println("receipt sent")
	return nil
}

func main() {
	ctx := context.Background()

	var errUser, errCart, errPayment, errReceipt error
	userDone := make(chan struct{})
	cartDone := make(chan struct{})
	paymentDone := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer func() {
			close(userDone)
			wg.Done()
		}()
		errUser = fetchUser(ctx)
	}()

	go func() {
		defer func() {
			close(cartDone)
			wg.Done()
		}()
		errCart = fetchCart(ctx)
	}()

	go func() {
		defer func() {
			close(paymentDone)
			wg.Done()
		}()
		<-userDone
		<-cartDone
		if errUser != nil || errCart != nil {
			return
		}
		errPayment = processPayment(ctx)
	}()

	go func() {
		defer wg.Done()
		<-paymentDone
		if errPayment != nil {
			return
		}
		errReceipt = sendReceipt(ctx)
	}()

	wg.Wait()
	if err := errors.Join(errUser, errCart, errPayment, errReceipt); err != nil {
		panic(err)
	}
}
