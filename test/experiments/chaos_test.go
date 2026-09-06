package experiments_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntkwan/go-flow"
)

var (
	ErrNetworkTimeout       = errors.New("network timeout")
	ErrPrimaryGatewayDown   = errors.New("primary payment gateway down")
	ErrInsufficientStock    = errors.New("insufficient inventory stock")
	ErrInvalidPaymentMethod = errors.New("invalid payment method")
)

type CheckoutContext struct {
	context.Context
	OrderID          string
	Amount           int64
	CartValidated    bool
	InventoryLocked  bool
	FraudScore       int
	PaymentCompleted bool
	PaymentProvider  string
	ShippingNotified bool
	AuditRecorded    bool
}

func TestChaosSelfHealingRetry(t *testing.T) {
	ctx := &CheckoutContext{
		Context: context.Background(),
		OrderID: "ord-101",
		Amount:  500,
	}

	var paymentAttempts atomic.Int32

	validateCart := flow.Node("validate_cart", func(c *CheckoutContext) error {
		c.CartValidated = true
		return nil
	})

	reserveInventory := flow.Node("reserve_inventory", func(c *CheckoutContext) error {
		c.InventoryLocked = true
		return nil
	}).After("validate_cart")

	fraudCheck := flow.Node("fraud_check", func(c *CheckoutContext) error {
		c.FraudScore = 12
		return nil
	}).After("validate_cart")

	processPayment := flow.Node("process_payment", func(c *CheckoutContext) error {
		attempt := paymentAttempts.Add(1)
		if attempt < 3 {
			return ErrNetworkTimeout
		}
		c.PaymentCompleted = true
		c.PaymentProvider = "primary"
		return nil
	}).After("reserve_inventory", "fraud_check").WithRetry(3, 2*time.Millisecond)

	notifyShipping := flow.Node("notify_shipping", func(c *CheckoutContext) error {
		c.ShippingNotified = true
		return nil
	}).After("process_payment")

	dag := flow.DAGWithReport(validateCart, reserveInventory, fraudCheck, processPayment, notifyShipping)
	report, err := dag(ctx)
	if err != nil {
		t.Fatalf("expected workflow to succeed via retry, got: %v", err)
	}

	if paymentAttempts.Load() != 3 {
		t.Fatalf("expected 3 payment attempts, got %d", paymentAttempts.Load())
	}
	if !cPaymentSuccess(ctx) {
		t.Fatalf("expected payment and shipping to be completed")
	}

	if len(report.Failed()) != 0 {
		t.Fatalf("expected 0 failed nodes in report, got %d", len(report.Failed()))
	}
	if len(report.Successful()) != 5 {
		t.Fatalf("expected 5 successful nodes, got %d", len(report.Successful()))
	}
}

func TestChaosFallbackRecovery(t *testing.T) {
	ctx := &CheckoutContext{
		Context: context.Background(),
		OrderID: "ord-102",
		Amount:  1200,
	}

	secondaryPayment := func(c *CheckoutContext) error {
		c.PaymentCompleted = true
		c.PaymentProvider = "secondary-backup"
		return nil
	}

	validateCart := flow.Node("validate_cart", func(c *CheckoutContext) error {
		c.CartValidated = true
		return nil
	})

	processPayment := flow.Node("process_payment", func(c *CheckoutContext) error {
		return ErrPrimaryGatewayDown
	}).After("validate_cart").WithFallback(secondaryPayment)

	notifyShipping := flow.Node("notify_shipping", func(c *CheckoutContext) error {
		c.ShippingNotified = true
		return nil
	}).After("process_payment")

	dag := flow.DAGWithReport(validateCart, processPayment, notifyShipping)
	report, err := dag(ctx)
	if err != nil {
		t.Fatalf("expected fallback to prevent workflow failure, got: %v", err)
	}

	if ctx.PaymentProvider != "secondary-backup" {
		t.Fatalf("expected secondary-backup provider, got %s", ctx.PaymentProvider)
	}
	if !ctx.ShippingNotified {
		t.Fatalf("expected shipping notification to proceed")
	}
	if len(report.Failed()) != 0 {
		t.Fatalf("expected 0 failed nodes due to fallback, got %d", len(report.Failed()))
	}
}

func TestChaosTimeoutIsolation(t *testing.T) {
	ctx := &CheckoutContext{
		Context: context.Background(),
		OrderID: "ord-103",
		Amount:  750,
	}

	validateCart := flow.Node("validate_cart", func(c *CheckoutContext) error {
		c.CartValidated = true
		return nil
	})

	reserveInventory := flow.Node("reserve_inventory", func(c *CheckoutContext) error {
		time.Sleep(50 * time.Millisecond)
		c.InventoryLocked = true
		return nil
	}).After("validate_cart").WithTimeout(10 * time.Millisecond)

	processPayment := flow.Node("process_payment", func(c *CheckoutContext) error {
		c.PaymentCompleted = true
		return nil
	}).After("reserve_inventory")

	notifyShipping := flow.Node("notify_shipping", func(c *CheckoutContext) error {
		c.ShippingNotified = true
		return nil
	}).After("process_payment")

	dag := flow.DAGWithReport(validateCart, reserveInventory, processPayment, notifyShipping)
	report, err := dag(ctx)
	if err == nil {
		t.Fatalf("expected deadline exceeded error from inventory timeout")
	}

	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(report.Node("reserve_inventory").Err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded error, got %v", err)
	}

	if report.Node("validate_cart").Status != flow.NodeStatusSuccess {
		t.Fatalf("expected validate_cart to succeed")
	}
	if report.Node("reserve_inventory").Status != flow.NodeStatusFailed {
		t.Fatalf("expected reserve_inventory to fail")
	}
	if report.Node("process_payment").Status != "" {
		t.Fatalf("expected process_payment to be unexecuted, got %v", report.Node("process_payment").Status)
	}
	if report.Node("notify_shipping").Status != "" {
		t.Fatalf("expected notify_shipping to be unexecuted, got %v", report.Node("notify_shipping").Status)
	}
	if ctx.PaymentCompleted {
		t.Fatalf("expected payment not to be executed after inventory failure")
	}
}

func TestChaosPanicContainment(t *testing.T) {
	ctx := &CheckoutContext{
		Context: context.Background(),
		OrderID: "ord-104",
		Amount:  300,
	}

	validateCart := flow.Node("validate_cart", func(c *CheckoutContext) error {
		c.CartValidated = true
		return nil
	})

	fraudCheck := flow.Node("fraud_check", flow.Step[*CheckoutContext](func(c *CheckoutContext) error {
		panic("nil pointer dereference in fraud scoring model")
	}).Wrap(flow.Recovery[*CheckoutContext]())).After("validate_cart")

	dag := flow.DAGWithReport(validateCart, fraudCheck)
	report, err := dag(ctx)
	if err == nil {
		t.Fatalf("expected error from recovered panic")
	}

	if report.Node("fraud_check").Status != flow.NodeStatusFailed {
		t.Fatalf("expected fraud_check to be marked FAILED, got %v", report.Node("fraud_check").Status)
	}
}

func TestChaosConditionalSkipping(t *testing.T) {
	ctx := &CheckoutContext{
		Context: context.Background(),
		OrderID: "ord-105",
		Amount:  50,
	}

	validateCart := flow.Node("validate_cart", func(c *CheckoutContext) error {
		c.CartValidated = true
		return nil
	})

	fraudCheck := flow.Node("fraud_check", func(c *CheckoutContext) error {
		c.FraudScore = 5
		return nil
	}).After("validate_cart").When(func(c *CheckoutContext) bool {
		return c.Amount > 100
	})

	processPayment := flow.Node("process_payment", func(c *CheckoutContext) error {
		c.PaymentCompleted = true
		return nil
	}).After("fraud_check")

	dag := flow.DAGWithReport(validateCart, fraudCheck, processPayment)
	report, err := dag(ctx)
	if err != nil {
		t.Fatalf("expected dag to succeed with skipped fraud check, got %v", err)
	}

	if report.Node("fraud_check").Status != flow.NodeStatusSkipped {
		t.Fatalf("expected fraud_check to be SKIPPED, got %v", report.Node("fraud_check").Status)
	}
	if !ctx.PaymentCompleted {
		t.Fatalf("expected payment to proceed despite skipped upstream node")
	}
}

func TestChaosHighConcurrencyBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high-concurrency chaos batch in short mode")
	}

	const totalOrders = 1000
	var successCount atomic.Int64
	var fallbackCount atomic.Int64
	var timeoutCount atomic.Int64
	var panicCount atomic.Int64

	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()

	var wg sync.WaitGroup
	start := time.Now()

	for i := range totalOrders {
		wg.Add(1)
		go func(orderID int) {
			defer wg.Done()

			faultType := orderID % 5
			ctx := &CheckoutContext{
				Context: context.Background(),
				OrderID: fmt.Sprintf("ord-%d", orderID),
				Amount:  int64(100 + rand.IntN(500)),
			}

			var paymentAttempts atomic.Int32

			validateCart := flow.Node("validate_cart", func(c *CheckoutContext) error {
				c.CartValidated = true
				return nil
			})

			reserveInventory := flow.Node("reserve_inventory", func(c *CheckoutContext) error {
				if faultType == 1 {
					time.Sleep(25 * time.Millisecond)
				}
				c.InventoryLocked = true
				return nil
			}).After("validate_cart").WithTimeout(10 * time.Millisecond)

			fraudCheck := flow.Node("fraud_check", flow.Step[*CheckoutContext](func(c *CheckoutContext) error {
				if faultType == 2 {
					panic("intermittent fraud service panic")
				}
				c.FraudScore = 15
				return nil
			}).Wrap(flow.Recovery[*CheckoutContext]())).After("validate_cart")

			secondaryPayment := func(c *CheckoutContext) error {
				c.PaymentCompleted = true
				c.PaymentProvider = "backup"
				fallbackCount.Add(1)
				return nil
			}

			processPayment := flow.Node("process_payment", func(c *CheckoutContext) error {
				attempts := paymentAttempts.Add(1)
				if faultType == 3 && attempts < 3 {
					return ErrNetworkTimeout
				}
				if faultType == 4 {
					return ErrPrimaryGatewayDown
				}
				c.PaymentCompleted = true
				c.PaymentProvider = "primary"
				return nil
			}).After("reserve_inventory", "fraud_check").
				WithRetry(3, 1*time.Millisecond).
				WithFallback(secondaryPayment)

			notifyShipping := flow.Node("notify_shipping", func(c *CheckoutContext) error {
				c.ShippingNotified = true
				return nil
			}).After("process_payment")

			dag := flow.DAGWithReport(validateCart, reserveInventory, fraudCheck, processPayment, notifyShipping)
			report, err := dag(ctx)

			if err == nil {
				successCount.Add(1)
			} else {
				switch faultType {
				case 1:
					timeoutCount.Add(1)
				case 2:
					panicCount.Add(1)
				}
			}
			_ = report
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	finalGoroutines := runtime.NumGoroutine()

	t.Logf("Chaos Batch Complete: Total=%d in %v, Success=%d, Fallback=%d, TimeoutAborts=%d, PanicAborts=%d",
		totalOrders, duration, successCount.Load(), fallbackCount.Load(), timeoutCount.Load(), panicCount.Load())
	t.Logf("Goroutines Before: %d, After: %d", initialGoroutines, finalGoroutines)

	if finalGoroutines-initialGoroutines > 10 {
		t.Fatalf("potential goroutine leak: before=%d, after=%d", initialGoroutines, finalGoroutines)
	}
}

func cPaymentSuccess(ctx *CheckoutContext) bool {
	return ctx.PaymentCompleted && ctx.ShippingNotified
}
