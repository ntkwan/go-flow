package main

import (
	"context"
	"fmt"
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
	ctx := context.Background()

	if err := validateInput(ctx); err != nil {
		panic(err)
	}

	if err := saveDatabase(ctx); err != nil {
		panic(err)
	}

	if err := sendNotification(ctx); err != nil {
		panic(err)
	}
}
