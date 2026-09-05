package main

import (
	"context"
	"fmt"

	"github.com/ntkwan/go-flow"
)

type userContext struct {
	context.Context
	IsPremium bool
}

func standardFlow(ctx *userContext) error {
	fmt.Println("executing standard flow")
	return nil
}

func premiumFlow(ctx *userContext) error {
	fmt.Println("executing premium VIP flow")
	return nil
}

func main() {
	workflow := flow.Branch(
		func(ctx *userContext) bool { return ctx.IsPremium },
		flow.Step[*userContext](premiumFlow),
		flow.Step[*userContext](standardFlow),
	)

	ctx := &userContext{
		Context:   context.Background(),
		IsPremium: true,
	}

	if err := workflow(ctx); err != nil {
		panic(err)
	}
}
