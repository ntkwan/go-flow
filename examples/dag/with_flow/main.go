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
	userNode := flow.Node("fetch_user", fetchUser)
	cartNode := flow.Node("fetch_cart", fetchCart)
	paymentNode := flow.Node("payment", processPayment).After("fetch_user", "fetch_cart")
	receiptNode := flow.Node("receipt", sendReceipt).After("payment")

	graph := flow.DAG(userNode, cartNode, paymentNode, receiptNode)
	if err := graph(context.Background()); err != nil {
		panic(err)
	}
}
