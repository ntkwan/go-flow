package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/ntkwan/go-flow"
)

func fetchUser(ctx context.Context) error {
	return nil
}

func checkInventory(ctx context.Context) error {
	return nil
}

func processPayment(ctx context.Context) error {
	return nil
}

func generateInvoice(ctx context.Context) error {
	return nil
}

func notifyCustomer(ctx context.Context) error {
	return nil
}

func main() {
	n1 := flow.Node("user", fetchUser)
	n2 := flow.Node("inventory", checkInventory)
	n3 := flow.Node("payment", processPayment).After("user", "inventory")
	n4 := flow.Node("invoice", generateInvoice).After("payment")
	n5 := flow.Node("notify", notifyCustomer).After("invoice")

	plan := flow.NewDAG(n1, n2, n3, n4, n5)

	if err := plan.Validate(); err != nil {
		if errors.Is(err, flow.ErrDAGCycle) {
			panic(fmt.Sprintf("Cycle detected: %v", err))
		}
		panic(err)
	}

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

	edgeMermaid, err := flow.DAGEdgesToMermaid(
		flow.From(fetchUser).To(processPayment),
		flow.From(checkInventory).To(processPayment),
		flow.From(processPayment).To(generateInvoice),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("Generated Edges Mermaid Diagram:")
	fmt.Println(edgeMermaid)
}
