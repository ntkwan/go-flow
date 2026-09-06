package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

// OrderContext represents OrderContext.
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
	orderCtx := &OrderContext{
		Context: context.Background(),
		UserID:  "USR-7701",
		OrderID: "ORD-9912",
		IsVIP:   true,
	}

	userNode := flow.Node("user",
		flow.Step[*OrderContext](fetchUser).Retry(3, 10*time.Millisecond),
	)
	invNode := flow.Node("inventory",
		flow.Step[*OrderContext](checkInventory).Timeout(1*time.Second),
	)

	vipStep := flow.Step[*OrderContext](applyVIPDiscount).When(func(c *OrderContext) bool {
		return c.IsVIP
	})
	vipNode := flow.Node("vip-discount", vipStep).After("user")

	enrichmentPipeline := flow.Seq(
		flow.Step[*OrderContext](enrichGeoLocation),
		flow.Step[*OrderContext](enrichFraudScore),
	)
	enrichNode := flow.Node("enrichment", enrichmentPipeline).After("user")

	resilientPayment := flow.Step[*OrderContext](primaryPaymentGateway).
		Fallback(backupPaymentGateway).
		Retry(2, 20*time.Millisecond).
		Timeout(2 * time.Second)

	paymentNode := flow.Node("payment", resilientPayment).
		After("inventory", "vip-discount", "enrichment")

	invoiceNode := flow.Node("invoice", generateInvoice).After("payment")
	dispatchNode := flow.Node("dispatch", dispatchWarehouse).After("payment")
	notifyNode := flow.Node("notify", flow.Step[*OrderContext](notifyCustomer).Once()).
		After("invoice", "dispatch")

	plan := flow.NewDAG(
		userNode, invNode, vipNode, enrichNode,
		paymentNode, invoiceNode, dispatchNode, notifyNode,
	)

	if err := plan.Validate(); err != nil {
		panic(err)
	}

	workflow := plan.StepN(4)
	if err := workflow.Exec(orderCtx); err != nil {
		panic(err)
	}

	fmt.Printf("Order %+v processed successfully\n", orderCtx)
}
