package main

import (
	"context"
	"fmt"
)

type userContext struct {
	context.Context
	Tier string
}

func freeTierWorkflow(ctx *userContext) error {
	fmt.Println("executing free tier workflow")
	return nil
}

func proTierWorkflow(ctx *userContext) error {
	fmt.Println("executing pro tier workflow")
	return nil
}

func enterpriseTierWorkflow(ctx *userContext) error {
	fmt.Println("executing enterprise VIP tier workflow")
	return nil
}

func main() {
	ctx := &userContext{
		Context: context.Background(),
		Tier:    "enterprise",
	}

	var err error
	switch ctx.Tier {
	case "enterprise":
		err = enterpriseTierWorkflow(ctx)
	case "pro":
		err = proTierWorkflow(ctx)
	default:
		err = freeTierWorkflow(ctx)
	}

	if err != nil {
		panic(err)
	}
}
