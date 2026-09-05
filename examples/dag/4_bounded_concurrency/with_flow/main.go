package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ntkwan/go-flow"
)

func task1(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Task 1 completed")
	return nil
}

func task2(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Task 2 completed")
	return nil
}

func task3(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Task 3 completed")
	return nil
}

func aggregateTask(ctx context.Context) error {
	fmt.Println("Aggregation task completed")
	return nil
}

func main() {
	ctx := context.Background()

	n1 := flow.Node("task1", task1)
	n2 := flow.Node("task2", task2)
	n3 := flow.Node("task3", task3)
	n4 := flow.Node("aggregator", aggregateTask).After("task1", "task2", "task3")

	boundedDAG := flow.DAGN(2, n1, n2, n3, n4)
	if err := boundedDAG(ctx); err != nil {
		panic(err)
	}

	boundedEdges := flow.DAGEdgesN(2,
		flow.From(task1).To(aggregateTask),
		flow.From(task2).To(aggregateTask),
		flow.From(task3).To(aggregateTask),
	)
	if err := boundedEdges(ctx); err != nil {
		panic(err)
	}
}
