package main

import (
	"context"
	"fmt"
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
	ctx := &userContext{
		Context:   context.Background(),
		IsPremium: true,
	}

	var err error
	if ctx.IsPremium {
		err = premiumFlow(ctx)
	} else {
		err = standardFlow(ctx)
	}

	if err != nil {
		panic(err)
	}
}
