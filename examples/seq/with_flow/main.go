package main

import (
	"context"
	"fmt"

	"github.com/ntkwan/go-flow"
)

func validateInput(ctx context.Context) error {
	fmt.Println("step 1: input validated")
	return nil
}

func saveDatabase(ctx context.Context) error {
	fmt.Println("step 2: record saved to database")
	return nil
}

func sendNotification(ctx context.Context) error {
	fmt.Println("step 3: notification sent")
	return nil
}

func main() {
	workflow := flow.Seq(
		validateInput,
		saveDatabase,
		sendNotification,
	)

	if err := workflow(context.Background()); err != nil {
		panic(err)
	}
}
