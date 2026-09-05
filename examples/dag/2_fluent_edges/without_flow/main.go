package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

func fetchUser(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("1. User profile loaded")
	return nil
}

func checkInventory(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("2. Inventory stock verified")
	return nil
}

func processPayment(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("3. Payment processed")
	return nil
}

func generateInvoice(ctx context.Context) error {
	fmt.Println("4. Invoice generated")
	return nil
}

func notifyCustomer(ctx context.Context) error {
	fmt.Println("5. Notification sent")
	return nil
}

func main() {
	ctx := context.Background()

	var errUser, errInventory, errPayment, errInvoice, errNotify error
	userDone := make(chan struct{})
	inventoryDone := make(chan struct{})
	paymentDone := make(chan struct{})
	invoiceDone := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(5)

	go func() {
		defer func() {
			close(userDone)
			wg.Done()
		}()
		errUser = fetchUser(ctx)
	}()

	go func() {
		defer func() {
			close(inventoryDone)
			wg.Done()
		}()
		errInventory = checkInventory(ctx)
	}()

	go func() {
		defer func() {
			close(paymentDone)
			wg.Done()
		}()
		<-userDone
		<-inventoryDone
		if errUser != nil || errInventory != nil {
			return
		}
		errPayment = processPayment(ctx)
	}()

	go func() {
		defer func() {
			close(invoiceDone)
			wg.Done()
		}()
		<-paymentDone
		if errPayment != nil {
			return
		}
		errInvoice = generateInvoice(ctx)
	}()

	go func() {
		defer wg.Done()
		<-invoiceDone
		if errInvoice != nil {
			return
		}
		errNotify = notifyCustomer(ctx)
	}()

	wg.Wait()
	if err := errors.Join(errUser, errInventory, errPayment, errInvoice, errNotify); err != nil {
		panic(err)
	}
}
