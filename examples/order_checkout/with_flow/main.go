package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

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
	chargeGateway := flow.Step[*OrderContext](func(c *OrderContext) error {
		c.Paid = true
		fmt.Printf("[5/8] Successfully charged $%.2f to credit card\n", c.FinalAmount)
		return nil
	})
	resilientPayment := chargeGateway.Retry(3, 20*time.Millisecond)
	return resilientPayment(ctx)
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

	validateNode := flow.Node("validate-order", validateOrder)
	userNode := flow.Node("fetch-user", fetchUser).After("validate-order")
	inventoryNode := flow.Node("fetch-inventory", fetchInventory).After("validate-order")
	discountNode := flow.Node("calculate-discounts", calculateDiscounts).After("fetch-user")
	paymentNode := flow.Node("process-payment", processPayment).After("calculate-discounts", "fetch-inventory")
	updateInvNode := flow.Node("update-inventory", updateInventory).After("process-payment")
	invoiceNode := flow.Node("generate-invoice", generateInvoice).After("process-payment")
	notifyNode := flow.Node("notify-customer", notifyCustomer).After("generate-invoice")
	dispatchNode := flow.Node("dispatch-warehouse", dispatchWarehouse).After("update-inventory")

	plan := flow.NewDAG(
		validateNode,
		userNode,
		inventoryNode,
		discountNode,
		paymentNode,
		updateInvNode,
		invoiceNode,
		notifyNode,
		dispatchNode,
	)

	mermaid, err := plan.ToMermaid()
	if err != nil {
		panic(err)
	}

	fmt.Println("=== Generated Mermaid Workflow Diagram ===")
	fmt.Println(mermaid)
	fmt.Println("==========================================")
	fmt.Println("\n=== Executing Workflow ===")

	pipeline := plan.Step()
	if err := pipeline(orderCtx); err != nil {
		panic(err)
	}

	fmt.Println("=== Order Checkout Flow Completed Successfully ===")
}
