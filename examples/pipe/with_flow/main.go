package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ntkwan/go-flow"
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
	workflow := flow.Pipe(
		transformValue,
		func(ctx *calculationContext) int { return ctx.InputVal },
		func(ctx *calculationContext, out string) { ctx.Result = out },
	)

	ctx := &calculationContext{
		Context:  context.Background(),
		InputVal: 42,
	}

	if err := workflow(ctx); err != nil {
		panic(err)
	}

	fmt.Printf("Calculation result: %s\n", ctx.Result)
}
