package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

func fetchUser(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("fetchUser executed")
	return nil
}

func fetchCart(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("fetchCart executed")
	return nil
}

func processPayment(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("processPayment executed")
	return nil
}

func sendReceipt(ctx context.Context) error {
	fmt.Println("sendReceipt executed")
	return nil
}

func runWithPureFunctionEdges(ctx context.Context) {
	fmt.Println("--- Style 1: Pure Function Fluent Edges (From.To) ---")
	graph := flow.DAGEdges(
		flow.From(fetchUser).To(processPayment),
		flow.From(fetchCart).To(processPayment),
		flow.From(processPayment).To(sendReceipt),
	)
	if err := graph(ctx); err != nil {
		panic(err)
	}
}

func runWithEdgeHelper(ctx context.Context) {
	fmt.Println("--- Style 2: Edge Connections (Edge) ---")
	graph := flow.DAGEdges(
		flow.Edge(fetchUser, processPayment),
		flow.Edge(fetchCart, processPayment),
		flow.Edge(processPayment, sendReceipt),
	)
	if err := graph(ctx); err != nil {
		panic(err)
	}
}

func runWithNamedNodes(ctx context.Context) {
	fmt.Println("--- Style 3: Named Nodes (Node.After) ---")
	userNode := flow.Node("fetch-user", fetchUser)
	cartNode := flow.Node("fetch-cart", fetchCart)
	paymentNode := flow.Node("process-payment", processPayment).After("fetch-user", "fetch-cart")
	receiptNode := flow.Node("send-receipt", sendReceipt).After("process-payment")

	graph := flow.DAG(userNode, cartNode, paymentNode, receiptNode)
	if err := graph(ctx); err != nil {
		panic(err)
	}
}

func runWithBoundedConcurrency(ctx context.Context) {
	fmt.Println("--- Style 4: Bounded Concurrency (DAGEdgesN / DAGN) ---")
	graph := flow.DAGEdgesN(2,
		flow.From(fetchUser).To(processPayment),
		flow.From(fetchCart).To(processPayment),
		flow.From(processPayment).To(sendReceipt),
	)
	if err := graph(ctx); err != nil {
		panic(err)
	}
}

func main() {
	ctx := context.Background()
	runWithPureFunctionEdges(ctx)
	runWithEdgeHelper(ctx)
	runWithNamedNodes(ctx)
	runWithBoundedConcurrency(ctx)
}
