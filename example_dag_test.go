package flow_test

import (
	"context"
	"fmt"

	"github.com/ntkwan/go-flow"
)

func ExampleDAG() {
	user := flow.Node("user", func(ctx context.Context) error {
		fmt.Println("User loaded")
		return nil
	})
	inventory := flow.Node("inventory", func(ctx context.Context) error {
		fmt.Println("Inventory reserved")
		return nil
	})
	payment := flow.Node("payment", func(ctx context.Context) error {
		fmt.Println("Payment charged")
		return nil
	}).After("user", "inventory")

	graph := flow.DAG(user, inventory, payment)
	_ = graph(context.Background())
}

func ExampleDAGEdges() {
	fetchUser := func(ctx context.Context) error {
		fmt.Println("Fetch user")
		return nil
	}
	chargePayment := func(ctx context.Context) error {
		fmt.Println("Charge payment")
		return nil
	}

	graph := flow.DAGEdges(
		flow.From(fetchUser).To(chargePayment),
	)

	_ = graph(context.Background())
}

func ExampleDAGN() {
	n1 := flow.Node("task1", func(ctx context.Context) error { return nil })
	n2 := flow.Node("task2", func(ctx context.Context) error { return nil })
	n3 := flow.Node("task3", func(ctx context.Context) error { return nil })

	boundedGraph := flow.DAGN(2, n1, n2, n3)
	_ = boundedGraph(context.Background())
}

func ExampleNewDAG() {
	user := flow.Node("user", func(ctx context.Context) error { return nil })
	payment := flow.Node("payment", func(ctx context.Context) error { return nil }).After("user")

	plan := flow.NewDAG(user, payment)
	if err := plan.Validate(); err != nil {
		panic(err)
	}

	step := plan.Step()
	_ = step(context.Background())
}

func ExampleDAGToMermaid() {
	user := flow.Node("user", func(ctx context.Context) error { return nil })
	payment := flow.Node("payment", func(ctx context.Context) error { return nil }).After("user")

	mermaid, err := flow.DAGToMermaid(user, payment)
	if err != nil {
		panic(err)
	}

	fmt.Println(len(mermaid) > 0)
}

func ExampleDAGWithReport() {
	user := flow.Node("user", func(ctx context.Context) error { return nil })
	payment := flow.Node("payment", func(ctx context.Context) error { return nil }).After("user")

	exec := flow.DAGWithReport(user, payment)
	report, err := exec(context.Background())
	if err != nil {
		panic(err)
	}

	for _, n := range report.Nodes {
		fmt.Printf("%s: %s\n", n.Name, n.Status)
	}
}

func ExampleDAG_conditional() {
	type userContext struct {
		context.Context
		isPremium bool
	}

	baseTask := flow.Node("base", func(ctx *userContext) error { return nil })
	bonusTask := flow.Node("bonus", func(ctx *userContext) error { return nil }).
		After("base").
		When(func(ctx *userContext) bool { return ctx.isPremium })

	graph := flow.DAG(baseTask, bonusTask)
	_ = graph(&userContext{Context: context.Background(), isPremium: false})
}
