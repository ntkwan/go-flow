package main

import (
	"context"
	"fmt"

	"github.com/ntkwan/go-flow"
)

func initializeConnection(ctx context.Context) error {
	fmt.Println("expensive connection pool initialized")
	return nil
}

func queryData(ctx context.Context) error {
	fmt.Println("query executed")
	return nil
}

func main() {
	initStep := flow.Once(initializeConnection)

	pipelineA := flow.Seq(initStep, queryData)
	pipelineB := flow.Seq(initStep, queryData)

	ctx := context.Background()
	_ = pipelineA(ctx)
	_ = pipelineB(ctx)
}
