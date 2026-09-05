package bench_test

import (
	"context"
	"testing"

	"github.com/ntkwan/go-flow"
)

func BenchmarkDAGSequential(b *testing.B) {
	nA := flow.Node("A", func(ctx context.Context) error { return nil })
	nB := flow.Node("B", func(ctx context.Context) error { return nil }).After("A")
	nC := flow.Node("C", func(ctx context.Context) error { return nil }).After("B")
	step := flow.DAG(nA, nB, nC)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDAGParallel(b *testing.B) {
	n1 := flow.Node("1", func(ctx context.Context) error { return nil })
	n2 := flow.Node("2", func(ctx context.Context) error { return nil })
	n3 := flow.Node("3", func(ctx context.Context) error { return nil })
	n4 := flow.Node("4", func(ctx context.Context) error { return nil })
	step := flow.DAG(n1, n2, n3, n4)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDAGDiamond(b *testing.B) {
	nA := flow.Node("A", func(ctx context.Context) error { return nil })
	nB := flow.Node("B", func(ctx context.Context) error { return nil }).After("A")
	nC := flow.Node("C", func(ctx context.Context) error { return nil }).After("A")
	nD := flow.Node("D", func(ctx context.Context) error { return nil }).After("B", "C")
	step := flow.DAG(nA, nB, nC, nD)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDAGOrderCheckout(b *testing.B) {
	validateNode := flow.Node("validate-order", func(ctx context.Context) error { return nil })
	userNode := flow.Node("fetch-user", func(ctx context.Context) error { return nil }).After("validate-order")
	inventoryNode := flow.Node("fetch-inventory", func(ctx context.Context) error { return nil }).After("validate-order")
	discountNode := flow.Node("calculate-discounts", func(ctx context.Context) error { return nil }).After("fetch-user")
	paymentNode := flow.Node("process-payment", func(ctx context.Context) error { return nil }).After("calculate-discounts", "fetch-inventory")
	updateInvNode := flow.Node("update-inventory", func(ctx context.Context) error { return nil }).After("process-payment")
	invoiceNode := flow.Node("generate-invoice", func(ctx context.Context) error { return nil }).After("process-payment")
	auditNode := flow.Node("audit-log", func(ctx context.Context) error { return nil }).After("process-payment")
	notifyNode := flow.Node("notify-customer", func(ctx context.Context) error { return nil }).After("generate-invoice")
	dispatchNode := flow.Node("dispatch-warehouse", func(ctx context.Context) error { return nil }).After("update-inventory")

	step := flow.DAG(
		validateNode, userNode, inventoryNode, discountNode, paymentNode,
		updateInvNode, invoiceNode, auditNode, notifyNode, dispatchNode,
	)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDAGN(b *testing.B) {
	nA := flow.Node("A", func(ctx context.Context) error { return nil })
	nB := flow.Node("B", func(ctx context.Context) error { return nil }).After("A")
	nC := flow.Node("C", func(ctx context.Context) error { return nil }).After("A")
	nD := flow.Node("D", func(ctx context.Context) error { return nil }).After("B", "C")
	step := flow.DAGN(2, nA, nB, nC, nD)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := step(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
