package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type OrderContext struct {
	context.Context
	UserID     string
	OrderID    string
	IsVIP      bool
	Discount   float64
	Paid       bool
	InvoiceID  string
	Dispatched bool
}

func fetchUser(ctx *OrderContext) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Printf("1. User profile loaded: %s (VIP: %v)\n", ctx.UserID, ctx.IsVIP)
	return nil
}

func checkInventory(ctx *OrderContext) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("2. Stock verified in warehouse")
	return nil
}

func applyVIPDiscount(ctx *OrderContext) error {
	ctx.Discount = 20.0
	fmt.Printf("3. VIP discount applied: $%.2f\n", ctx.Discount)
	return nil
}

func enrichGeoLocation(ctx *OrderContext) error {
	fmt.Println("4a. Geo-location resolved")
	return nil
}

func enrichFraudScore(ctx *OrderContext) error {
	fmt.Println("4b. Anti-fraud risk evaluated")
	return nil
}

func primaryPaymentGateway(ctx *OrderContext) error {
	time.Sleep(10 * time.Millisecond)
	ctx.Paid = true
	fmt.Println("5. Primary gateway processed charge")
	return nil
}

func backupPaymentGateway(ctx *OrderContext) error {
	ctx.Paid = true
	fmt.Println("5-backup. Secondary payment gateway processed charge")
	return nil
}

func generateInvoice(ctx *OrderContext) error {
	ctx.InvoiceID = "INV-" + ctx.OrderID
	fmt.Printf("6. Invoice generated: %s\n", ctx.InvoiceID)
	return nil
}

func dispatchWarehouse(ctx *OrderContext) error {
	ctx.Dispatched = true
	fmt.Println("7. Warehouse package dispatched")
	return nil
}

func notifyCustomer(ctx *OrderContext) error {
	fmt.Println("8. Push notification sent")
	return nil
}

func main() {
	ctx := &OrderContext{
		Context: context.Background(),
		UserID:  "USR-7701",
		OrderID: "ORD-9912",
		IsVIP:   true,
	}

	var errUser, errInv, errVIP, errEnrich, errPayment, errInvoice, errDispatch, errNotify error

	userDone := make(chan struct{})
	invDone := make(chan struct{})
	vipDone := make(chan struct{})
	enrichDone := make(chan struct{})
	paymentDone := make(chan struct{})
	invoiceDone := make(chan struct{})
	dispatchDone := make(chan struct{})

	var onceNotify sync.Once
	var wg sync.WaitGroup
	wg.Add(8)

	go func() {
		defer func() {
			close(userDone)
			wg.Done()
		}()
		for attempt := 0; attempt <= 3; attempt++ {
			if errUser = fetchUser(ctx); errUser == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		defer func() {
			close(invDone)
			wg.Done()
		}()
		tctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- checkInventory(ctx) }()
		select {
		case <-tctx.Done():
			errInv = tctx.Err()
		case errInv = <-done:
		}
	}()

	go func() {
		defer func() {
			close(vipDone)
			wg.Done()
		}()
		<-userDone
		if errUser != nil {
			return
		}
		if ctx.IsVIP {
			errVIP = applyVIPDiscount(ctx)
		}
	}()

	go func() {
		defer func() {
			close(enrichDone)
			wg.Done()
		}()
		<-userDone
		if errUser != nil {
			return
		}
		if errEnrich = enrichGeoLocation(ctx); errEnrich == nil {
			errEnrich = enrichFraudScore(ctx)
		}
	}()

	go func() {
		defer func() {
			close(paymentDone)
			wg.Done()
		}()
		<-invDone
		<-vipDone
		<-enrichDone
		if errInv != nil || errVIP != nil || errEnrich != nil {
			return
		}
		for attempt := 0; attempt <= 2; attempt++ {
			errPayment = primaryPaymentGateway(ctx)
			if errPayment == nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		if errPayment != nil {
			errPayment = backupPaymentGateway(ctx)
		}
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
		defer func() {
			close(dispatchDone)
			wg.Done()
		}()
		<-paymentDone
		if errPayment != nil {
			return
		}
		errDispatch = dispatchWarehouse(ctx)
	}()

	go func() {
		defer wg.Done()
		<-invoiceDone
		<-dispatchDone
		if errInvoice != nil || errDispatch != nil {
			return
		}
		onceNotify.Do(func() {
			errNotify = notifyCustomer(ctx)
		})
	}()

	wg.Wait()
	if err := errors.Join(errUser, errInv, errVIP, errEnrich, errPayment, errInvoice, errDispatch, errNotify); err != nil {
		panic(err)
	}
}
