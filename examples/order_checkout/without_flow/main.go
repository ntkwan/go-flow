package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// OrderContext represents OrderContext.
type OrderContext struct {
	context.Context
	OrderID     string
	UserID      string
	TotalAmount float64
	Discount    float64
	FinalAmount float64
	Paid        bool
}

func validateOrder(ctx *OrderContext) error {
	fmt.Printf("[1/8] Validating order %s for user %s\n", ctx.OrderID, ctx.UserID)
	time.Sleep(10 * time.Millisecond)
	return nil
}

func fetchUser(ctx *OrderContext) error {
	fmt.Println("[2/8] Fetching user profile and loyalty status")
	time.Sleep(15 * time.Millisecond)
	return nil
}

func fetchInventory(ctx *OrderContext) error {
	fmt.Println("[3/8] Checking and reserving warehouse inventory")
	time.Sleep(15 * time.Millisecond)
	return nil
}

func calculateDiscounts(ctx *OrderContext) error {
	ctx.Discount = 15.0
	ctx.FinalAmount = ctx.TotalAmount - ctx.Discount
	fmt.Printf("[4/8] Applied discounts: $%.2f. Final amount: $%.2f\n", ctx.Discount, ctx.FinalAmount)
	return nil
}

func processPayment(ctx *OrderContext) error {
	ctx.Paid = true
	fmt.Printf("[5/8] Successfully charged $%.2f to credit card\n", ctx.FinalAmount)
	return nil
}

func updateInventory(ctx *OrderContext) error {
	fmt.Println("[6/8] Committing inventory stock decrement")
	time.Sleep(10 * time.Millisecond)
	return nil
}

func generateInvoice(ctx *OrderContext) error {
	fmt.Println("[7/8] Generating digital PDF invoice")
	time.Sleep(10 * time.Millisecond)
	return nil
}

func notifyCustomer(ctx *OrderContext) error {
	fmt.Printf("[8/8] Notification email sent for order %s\n", ctx.OrderID)
	return nil
}

func dispatchWarehouse(ctx *OrderContext) error {
	fmt.Println("[8/8] Warehouse robot picking and packing dispatched")
	return nil
}

func main() {
	orderCtx := &OrderContext{
		Context:     context.Background(),
		OrderID:     "ORD-2026-9921",
		UserID:      "USR-402",
		TotalAmount: 150.0,
	}

	if err := validateOrder(orderCtx); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	var fetchErr error
	var errMu sync.Mutex

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := fetchUser(orderCtx); err != nil {
			errMu.Lock()
			if fetchErr == nil {
				fetchErr = err
			}
			errMu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		if err := fetchInventory(orderCtx); err != nil {
			errMu.Lock()
			if fetchErr == nil {
				fetchErr = err
			}
			errMu.Unlock()
		}
	}()
	wg.Wait()

	if fetchErr != nil {
		panic(fetchErr)
	}

	if err := calculateDiscounts(orderCtx); err != nil {
		panic(err)
	}

	if err := processPayment(orderCtx); err != nil {
		panic(err)
	}

	var postWg sync.WaitGroup
	var postErr error

	postWg.Add(2)
	go func() {
		defer postWg.Done()
		if err := updateInventory(orderCtx); err != nil {
			errMu.Lock()
			if postErr == nil {
				postErr = err
			}
			errMu.Unlock()
			return
		}
		if err := dispatchWarehouse(orderCtx); err != nil {
			errMu.Lock()
			if postErr == nil {
				postErr = err
			}
			errMu.Unlock()
		}
	}()

	go func() {
		defer postWg.Done()
		if err := generateInvoice(orderCtx); err != nil {
			errMu.Lock()
			if postErr == nil {
				postErr = err
			}
			errMu.Unlock()
			return
		}
		if err := notifyCustomer(orderCtx); err != nil {
			errMu.Lock()
			if postErr == nil {
				postErr = err
			}
			errMu.Unlock()
		}
	}()

	postWg.Wait()
	if postErr != nil {
		panic(postErr)
	}

	fmt.Printf("=== Order Checkout Flow for %s Completed Successfully ===\n", orderCtx.OrderID)
}
