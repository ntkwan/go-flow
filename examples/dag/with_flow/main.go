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
	fmt.Printf("fetchUser: user %s loaded (VIP: %v)\n", ctx.UserID, ctx.IsVIP)
	return nil
}

func checkInventory(ctx *PipelineContext) error {
	time.Sleep(10 * time.Millisecond)
	ctx.InventoryOK = true
	fmt.Println("checkInventory: stock verified and reserved")
	return nil
}

func fetchPricing(ctx *PipelineContext) error {
	time.Sleep(10 * time.Millisecond)
	ctx.PricingOK = true
	fmt.Println("fetchPricing: tax and rates calculated")
	return nil
}

func applyVIPDiscount(ctx *PipelineContext) error {
	fmt.Println("applyVIPDiscount: VIP loyalty discount applied")
	return nil
}

func enrichGeoData(ctx *PipelineContext) error {
	fmt.Println("enrichGeoData: IP location resolved")
	return nil
}

func enrichRiskScore(ctx *PipelineContext) error {
	fmt.Println("enrichRiskScore: anti-fraud score computed")
	return nil
}

func primaryPaymentGateway(ctx *PipelineContext) error {
	time.Sleep(10 * time.Millisecond)
	ctx.PaymentOK = true
	fmt.Println("primaryPaymentGateway: credit card charge succeeded")
	return nil
}

func backupPaymentGateway(ctx *PipelineContext) error {
	ctx.PaymentOK = true
	fmt.Println("backupPaymentGateway: secondary gateway charge succeeded")
	return nil
}

func generateInvoice(ctx *PipelineContext) error {
	ctx.InvoiceID = "INV-" + ctx.OrderID
	fmt.Printf("generateInvoice: generated %s\n", ctx.InvoiceID)
	return nil
}

func dispatchWarehouse(ctx *PipelineContext) error {
	ctx.Dispatched = true
	fmt.Println("dispatchWarehouse: package queued for packing")
	return nil
}

func notifyCustomer(ctx *PipelineContext) error {
	ctx.Notification = true
	fmt.Println("notifyCustomer: confirmation SMS dispatched")
	return nil
}

func style1NamedNodes(ctx *PipelineContext) {
	fmt.Println("=== Style 1: Named Nodes with flow.Node and .After() ===")

	userNode := flow.Node("user", fetchUser)
	inventoryNode := flow.Node("inventory", checkInventory)
	paymentNode := flow.Node("payment", primaryPaymentGateway).After("user", "inventory")
	invoiceNode := flow.Node("invoice", generateInvoice).After("payment")

	graph := flow.DAG(userNode, inventoryNode, paymentNode, invoiceNode)
	if err := graph(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Style 1 completed.")
	fmt.Println()
}

func style2FluentFunctionEdges(ctx *PipelineContext) {
	fmt.Println("=== Style 2: Fluent Pure-Function Edges with flow.From.To ===")

	graph := flow.DAGEdges(
		flow.From(fetchUser).To(primaryPaymentGateway),
		flow.From(checkInventory).To(primaryPaymentGateway),
		flow.From(primaryPaymentGateway).To(generateInvoice),
		flow.From(generateInvoice).To(notifyCustomer),
	)

	if err := graph(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Style 2 completed.")
	fmt.Println()
}

func style3PairwiseEdges(ctx *PipelineContext) {
	fmt.Println("=== Style 3: Pairwise Edges with flow.Edge ===")

	graph := flow.DAGEdges(
		flow.Edge(fetchUser, primaryPaymentGateway),
		flow.Edge(checkInventory, primaryPaymentGateway),
		flow.Edge(primaryPaymentGateway, generateInvoice),
		flow.Edge(primaryPaymentGateway, dispatchWarehouse),
	)

	if err := graph(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Style 3 completed.")
	fmt.Println()
}

func style4PreflightPlanValidation(ctx *PipelineContext) {
	fmt.Println("=== Style 4: Pre-flight Plan Validation (NewDAG / Validate) ===")

	n1 := flow.Node("user", fetchUser)
	n2 := flow.Node("inventory", checkInventory)
	n3 := flow.Node("payment", primaryPaymentGateway).After("user", "inventory")

	plan := flow.NewDAG(n1, n2, n3)
	if err := plan.Validate(); err != nil {
		panic(err)
	}

	workflow := plan.Step()
	if err := workflow(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Style 4 completed.")
	fmt.Println()
}

func style5BoundedConcurrency(ctx *PipelineContext) {
	fmt.Println("=== Style 5: Bounded Concurrency Worker Limits (DAGN / DAGEdgesN) ===")

	n1 := flow.Node("user", fetchUser)
	n2 := flow.Node("inventory", checkInventory)
	n3 := flow.Node("pricing", fetchPricing)
	n4 := flow.Node("payment", primaryPaymentGateway).After("user", "inventory", "pricing")

	stepWithWorkerLimit := flow.DAGN(2, n1, n2, n3, n4)
	if err := stepWithWorkerLimit(ctx); err != nil {
		panic(err)
	}

	edgeStepWithLimit := flow.DAGEdgesN(2,
		flow.From(fetchUser).To(primaryPaymentGateway),
		flow.From(checkInventory).To(primaryPaymentGateway),
		flow.From(fetchPricing).To(primaryPaymentGateway),
	)
	if err := edgeStepWithLimit(ctx); err != nil {
		panic(err)
	}

	fmt.Println("Style 5 completed.")
	fmt.Println()
}

func style6CompositeResilientDAG(ctx *PipelineContext) {
	fmt.Println("=== Style 6: Composite Resilient DAG (Decorators & Sub-Pipelines) ===")

	userNode := flow.Node("user",
		flow.Step[*PipelineContext](fetchUser).Retry(3, 10*time.Millisecond),
	)
	invNode := flow.Node("inventory",
		flow.Step[*PipelineContext](checkInventory).Timeout(1*time.Second),
	)
	pricingNode := flow.Node("pricing", fetchPricing)

	vipStep := flow.Step[*PipelineContext](applyVIPDiscount).When(func(c *PipelineContext) bool {
		return c.IsVIP
	})
	vipNode := flow.Node("vip-discount", vipStep).After("user", "pricing")

	enrichmentPipeline := flow.Seq(
		flow.Step[*PipelineContext](enrichGeoData),
		flow.Step[*PipelineContext](enrichRiskScore),
	)
	enrichNode := flow.Node("enrichment", enrichmentPipeline).After("user")

	resilientPayment := flow.Step[*PipelineContext](primaryPaymentGateway).
		Fallback(backupPaymentGateway).
		Retry(2, 20*time.Millisecond).
		Timeout(2 * time.Second)

	paymentNode := flow.Node("payment", resilientPayment).
		After("inventory", "vip-discount", "enrichment")

	invoiceNode := flow.Node("invoice", generateInvoice).After("payment")
	dispatchNode := flow.Node("dispatch", dispatchWarehouse).After("payment")
	notifyNode := flow.Node("notify", flow.Step[*PipelineContext](notifyCustomer).Once()).
		After("invoice", "dispatch")

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
	fmt.Println("Style 6 completed.")
	fmt.Println()
}

func style7VisualDiagramExport() {
	fmt.Println("=== Style 7: Visual Architecture Exports (Mermaid & DOT) ===")

	userNode := flow.Node("user", fetchUser)
	invNode := flow.Node("inventory", checkInventory)
	paymentNode := flow.Node("payment", primaryPaymentGateway).After("user", "inventory")
	invoiceNode := flow.Node("invoice", generateInvoice).After("payment")
	notifyNode := flow.Node("notify", notifyCustomer).After("invoice")

	plan := flow.NewDAG(userNode, invNode, paymentNode, invoiceNode, notifyNode)

	mermaid, err := plan.ToMermaid()
	if err != nil {
		panic(err)
	}
	fmt.Println("Mermaid Diagram Output:")
	fmt.Println(mermaid)

	dot, err := plan.ToDOT()
	if err != nil {
		panic(err)
	}
	fmt.Println("Graphviz DOT Output:")
	fmt.Println(dot)

	edgeMermaid, err := flow.DAGEdgesToMermaid(
		flow.From(fetchUser).To(primaryPaymentGateway),
		flow.From(checkInventory).To(primaryPaymentGateway),
		flow.From(primaryPaymentGateway).To(generateInvoice),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("Pure Function Edges Mermaid Diagram:")
	fmt.Println(edgeMermaid)

	edgeDOT, err := flow.DAGEdgesToDOT(
		flow.Edge(fetchUser, primaryPaymentGateway),
		flow.Edge(checkInventory, primaryPaymentGateway),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("Pure Function Edges DOT Graph:")
	fmt.Println(edgeDOT)
	fmt.Println("Style 7 completed.")
	fmt.Println()
}

func main() {
	ctx := &PipelineContext{
		Context: context.Background(),
		UserID:  "USR-4091",
		OrderID: "ORD-8820",
		IsVIP:   true,
	}

	style1NamedNodes(ctx)
	style2FluentFunctionEdges(ctx)
	style3PairwiseEdges(ctx)
	style4PreflightPlanValidation(ctx)
	style5BoundedConcurrency(ctx)
	style6CompositeResilientDAG(ctx)
	style7VisualDiagramExport()
}
