package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

type PipelineContext struct {
	context.Context
	UserID       string
	OrderID      string
	IsVIP        bool
	InventoryOK  bool
	PricingOK    bool
	PaymentOK    bool
	InvoiceID    string
	Dispatched   bool
	Notification bool
}

func fetchUser(ctx *PipelineContext) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Printf("[1] User profile fetched: %s (VIP: %v)\n", ctx.UserID, ctx.IsVIP)
	return nil
}

func checkInventory(ctx *PipelineContext) error {
	time.Sleep(10 * time.Millisecond)
	ctx.InventoryOK = true
	fmt.Println("[2] Warehouse inventory verified")
	return nil
}

func fetchPricing(ctx *PipelineContext) error {
	time.Sleep(10 * time.Millisecond)
	ctx.PricingOK = true
	fmt.Println("[3] Dynamic pricing and tax calculated")
	return nil
}

func applyVIPDiscount(ctx *PipelineContext) error {
	fmt.Println("[4] VIP discount applied to order")
	return nil
}

func enrichGeoData(ctx *PipelineContext) error {
	fmt.Println("[5a] Geo-location data enriched")
	return nil
}

func enrichRiskScore(ctx *PipelineContext) error {
	fmt.Println("[5b] Fraud risk score computed")
	return nil
}

func primaryPaymentGateway(ctx *PipelineContext) error {
	time.Sleep(10 * time.Millisecond)
	ctx.PaymentOK = true
	fmt.Println("[6] Primary payment gateway processed charge")
	return nil
}

func backupPaymentGateway(ctx *PipelineContext) error {
	ctx.PaymentOK = true
	fmt.Println("[6-backup] Secondary payment gateway processed charge")
	return nil
}

func generateInvoice(ctx *PipelineContext) error {
	ctx.InvoiceID = "INV-" + ctx.OrderID
	fmt.Printf("[7] Invoice generated: %s\n", ctx.InvoiceID)
	return nil
}

func dispatchWarehouse(ctx *PipelineContext) error {
	ctx.Dispatched = true
	fmt.Println("[8] Warehouse package dispatched")
	return nil
}

func notifyCustomer(ctx *PipelineContext) error {
	ctx.Notification = true
	fmt.Println("[9] Push notification and confirmation email sent")
	return nil
}

func runAdvancedCompositeDAG(ctx *PipelineContext) {
	fmt.Println("=== 1. Advanced Composite & Resilient DAG ===")

	userNode := flow.Node("fetch-user", flow.Step[*PipelineContext](fetchUser).Retry(3, 10*time.Millisecond))
	invNode := flow.Node("check-inventory", flow.Step[*PipelineContext](checkInventory).Timeout(1*time.Second))
	pricingNode := flow.Node("fetch-pricing", fetchPricing)

	vipStep := flow.Step[*PipelineContext](applyVIPDiscount).When(func(c *PipelineContext) bool {
		return c.IsVIP
	})
	vipNode := flow.Node("vip-discount", vipStep).After("fetch-user", "fetch-pricing")

	enrichmentPipeline := flow.Seq(
		flow.Step[*PipelineContext](enrichGeoData),
		flow.Step[*PipelineContext](enrichRiskScore),
	)
	enrichNode := flow.Node("enrich-metadata", enrichmentPipeline).After("fetch-user")

	resilientPayment := flow.Step[*PipelineContext](primaryPaymentGateway).
		Fallback(backupPaymentGateway).
		Retry(2, 20*time.Millisecond).
		Timeout(2 * time.Second)

	paymentNode := flow.Node("process-payment", resilientPayment).
		After("check-inventory", "vip-discount", "enrich-metadata")

	invoiceNode := flow.Node("generate-invoice", generateInvoice).After("process-payment")
	dispatchNode := flow.Node("dispatch-warehouse", dispatchWarehouse).After("process-payment")
	notifyNode := flow.Node("notify-customer", flow.Step[*PipelineContext](notifyCustomer).Once()).
		After("generate-invoice", "dispatch-warehouse")

	plan := flow.NewDAG(
		userNode, invNode, pricingNode, vipNode, enrichNode,
		paymentNode, invoiceNode, dispatchNode, notifyNode,
	)

	if err := plan.Validate(); err != nil {
		panic(err)
	}

	workflow := plan.StepN(4)
	if err := workflow(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Advanced DAG execution completed successfully.")
	fmt.Println()
}

func runFunctionalEdgeDAG(ctx *PipelineContext) {
	fmt.Println("=== 2. Functional Edges (From.To) ===")

	graph := flow.DAGEdges(
		flow.From(fetchUser).To(primaryPaymentGateway),
		flow.From(checkInventory).To(primaryPaymentGateway),
		flow.From(primaryPaymentGateway).To(generateInvoice),
		flow.From(generateInvoice).To(notifyCustomer),
	)

	if err := graph(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Functional edges DAG execution completed successfully.")
	fmt.Println()
}

func runGraphVisualizationAndInspection() {
	fmt.Println("=== 3. Graph Validation & Architecture Export ===")

	n1 := flow.Node("fetch-user", fetchUser)
	n2 := flow.Node("check-inventory", checkInventory)
	n3 := flow.Node("process-payment", primaryPaymentGateway).After("fetch-user", "check-inventory")
	n4 := flow.Node("generate-invoice", generateInvoice).After("process-payment")
	n5 := flow.Node("notify-customer", notifyCustomer).After("generate-invoice")

	plan := flow.NewDAG(n1, n2, n3, n4, n5)

	mermaid, err := plan.ToMermaid()
	if err != nil {
		panic(err)
	}
	fmt.Println("Generated Mermaid Diagram:")
	fmt.Println(mermaid)

	dot, err := plan.ToDOT()
	if err != nil {
		panic(err)
	}
	fmt.Println("Generated DOT Graph:")
	fmt.Println(dot)
}

func main() {
	ctx := &PipelineContext{
		Context: context.Background(),
		UserID:  "USR-9842",
		OrderID: "ORD-5501",
		IsVIP:   true,
	}

	runAdvancedCompositeDAG(ctx)
	runFunctionalEdgeDAG(ctx)
	runGraphVisualizationAndInspection()
}
