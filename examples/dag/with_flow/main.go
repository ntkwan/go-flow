package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

func fetchUser(ctx context.Context) error {
	time.Sleep(20 * time.Millisecond)
	fmt.Println("user fetched")
	return nil
}

func fetchCart(ctx context.Context) error {
	time.Sleep(20 * time.Millisecond)
	fmt.Println("cart fetched")
	return nil
}

func processPayment(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("payment processed")
	return nil
}

func sendReceipt(ctx context.Context) error {
	fmt.Println("receipt sent")
	return nil
}

func main() {
	graph := flow.DAGEdges(
		flow.Edge(fetchUser, processPayment),
		flow.Edge(fetchCart, processPayment),
		flow.Edge(processPayment, sendReceipt),
	)
	if err := graph(context.Background()); err != nil {
		panic(err)
	}
}
