package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

type orderContext struct {
	context.Context
	orderID      string
	amount       float64
	isVIP        bool
	discountAuth bool
}

func main() {
	validateOrder := func(ctx *orderContext) error {
		fmt.Printf("[Validate] Order %s for $%.2f\n", ctx.orderID, ctx.amount)
		return nil
	}

	applyVIPDiscount := func(ctx *orderContext) error {
		fmt.Printf("[VIP Discount] Applying 20%% VIP discount to %s\n", ctx.orderID)
		ctx.discountAuth = true
		return nil
	}

	chargeCard := func(ctx *orderContext) error {
		fmt.Printf("[Payment] Charging card for order %s (VIP discount applied: %t)\n", ctx.orderID, ctx.discountAuth)
		return nil
	}

	notifyCustomer := func(ctx *orderContext) error {
		fmt.Printf("[Notify] Confirmation sent for order %s\n", ctx.orderID)
		return nil
	}

	plan := flow.NewDAG(
		flow.Node("validate", validateOrder).WithTimeout(2*time.Second),
		flow.Node("vip_discount", applyVIPDiscount).
			After("validate").
			When(func(ctx *orderContext) bool { return ctx.isVIP }),
		flow.Node("payment", chargeCard).
			After("validate", "vip_discount").
			WithRetry(2, 50*time.Millisecond),
		flow.Node("notify", notifyCustomer).
			After("payment"),
	)

	fmt.Println("--- Standard Customer Order (VIP Discount Skipped) ---")
	stdOrder := &orderContext{
		Context: context.Background(),
		orderID: "ORD-1001",
		amount:  150.00,
		isVIP:   false,
	}

	report, err := plan.ExecWithReport(stdOrder)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\nDAG Execution Completed in %v\n", report.Duration)
	for _, n := range report.Nodes {
		fmt.Printf("  • %-15s [%s] in %v\n", n.Name, n.Status, n.Duration)
	}

	fmt.Println("\n--- VIP Customer Order (VIP Discount Applied) ---")
	vipOrder := &orderContext{
		Context: context.Background(),
		orderID: "ORD-9999",
		amount:  500.00,
		isVIP:   true,
	}

	vipReport, err := plan.ExecWithReport(vipOrder)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\nDAG Execution Completed in %v\n", vipReport.Duration)
	for _, n := range vipReport.Nodes {
		fmt.Printf("  • %-15s [%s] in %v\n", n.Name, n.Status, n.Duration)
	}
}
