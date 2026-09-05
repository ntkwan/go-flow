package main

import (
	"context"
	"fmt"
	"strconv"
)

type calculationContext struct {
	context.Context
	InputVal int
	Result   string
}

func transformValue(ctx *calculationContext, in int) (string, error) {
	return strconv.Itoa(in * 10), nil
}

func main() {
	ctx := &calculationContext{
		Context:  context.Background(),
		InputVal: 42,
	}

	res, err := transformValue(ctx, ctx.InputVal)
	if err != nil {
		panic(err)
	}
	ctx.Result = res

	fmt.Printf("Calculation result: %s\n", ctx.Result)
}
