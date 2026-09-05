package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

func fetchUser(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("1. User profile loaded")
	return nil
}

func checkInventory(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("2. Inventory stock verified")
	return nil
}

func processPayment(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("3. Payment processed")
	return nil
}

func generateInvoice(ctx context.Context) error {
	fmt.Println("4. Invoice generated")
	return nil
}

func main() {
	ctx := context.Background()

	userNode := flow.Node("user", fetchUser)
	inventoryNode := flow.Node("inventory", checkInventory)
	paymentNode := flow.Node("payment", processPayment).After("user", "inventory")
	invoiceNode := flow.Node("invoice", generateInvoice).After("payment")

	graph := flow.DAG(userNode, inventoryNode, paymentNode, invoiceNode)
	if err := graph(ctx); err != nil {
		panic(err)
	}
}
