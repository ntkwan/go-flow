package main

import (
	"context"
	"fmt"

	"github.com/ntkwan/go-flow"
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
	workflow := flow.Dynamic(func(ctx *userContext) flow.Step[*userContext] {
		switch ctx.Tier {
		case "enterprise":
			return enterpriseTierWorkflow
		case "pro":
			return proTierWorkflow
		default:
			return freeTierWorkflow
		}
	})

	ctx := &userContext{
		Context: context.Background(),
		Tier:    "enterprise",
	}

	if err := workflow(ctx); err != nil {
		panic(err)
	}
}
