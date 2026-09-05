package flow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDAGEmpty(t *testing.T) {
	step := DAG[context.Context]()
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestDAGSingleNode(t *testing.T) {
	var executed bool
	node := Node("single", func(ctx context.Context) error {
		executed = true
		return nil
	})

	step := DAG(node)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !executed {
		t.Fatal("expected single node to execute")
	}
}

func TestDAGLinearChain(t *testing.T) {
	var mu sync.Mutex
	var order []string

	n1 := Node("first", func(ctx context.Context) error {
		mu.Lock()
		order = append(order, "first")
		mu.Unlock()
		return nil
	})

	n2 := Node("second", func(ctx context.Context) error {
		mu.Lock()
		order = append(order, "second")
		mu.Unlock()
		return nil
	}).After("first")

	n3 := Node("third", func(ctx context.Context) error {
		mu.Lock()
		order = append(order, "third")
		mu.Unlock()
		return nil
	}).After("second")

	step := DAG(n3, n1, n2)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if len(order) != 3 || order[0] != "first" || order[1] != "second" || order[2] != "third" {
		t.Fatalf("expected order [first second third], got %v", order)
	}
}

func TestDAGDiamondConcurrent(t *testing.T) {
	var userDone, cartDone atomic.Bool
	var paymentExecuted atomic.Bool

	nUser := Node("user", func(ctx context.Context) error {
		time.Sleep(20 * time.Millisecond)
		userDone.Store(true)
		return nil
	})

	nCart := Node("cart", func(ctx context.Context) error {
		time.Sleep(20 * time.Millisecond)
		cartDone.Store(true)
		return nil
	})

	nPayment := Node("payment", func(ctx context.Context) error {
		if !userDone.Load() || !cartDone.Load() {
			t.Error("prerequisites user and cart must be done before payment")
		}
		paymentExecuted.Store(true)
		return nil
	}).After("user", "cart")

	start := time.Now()
	step := DAG(nUser, nCart, nPayment)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	elapsed := time.Since(start)
	if elapsed >= 45*time.Millisecond {
		t.Fatalf("expected concurrent user and cart execution under 45ms, took %v", elapsed)
	}
	if !paymentExecuted.Load() {
		t.Fatal("expected payment to execute")
	}
}

func TestDAGIndependentBranches(t *testing.T) {
	var count atomic.Int32
	nodes := []*DAGNode[context.Context]{
		Node("b1", func(ctx context.Context) error {
			count.Add(1)
			return nil
		}),
		Node("b2", func(ctx context.Context) error {
			count.Add(1)
			return nil
		}),
		Node("b3", func(ctx context.Context) error {
			count.Add(1)
			return nil
		}),
	}

	step := DAG(nodes...)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if count.Load() != 3 {
		t.Fatalf("expected 3 executions, got %d", count.Load())
	}
}

func TestDAGCycleDirect(t *testing.T) {
	nA := Node("A", func(ctx context.Context) error { return nil }).After("B")
	nB := Node("B", func(ctx context.Context) error { return nil }).After("A")

	step := DAG(nA, nB)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}
}

func TestDAGCycleSelf(t *testing.T) {
	nA := Node("A", func(ctx context.Context) error { return nil }).After("A")

	step := DAG(nA)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected self-cycle error, got nil")
	}
}

func TestDAGCycleTransitive(t *testing.T) {
	nA := Node("A", func(ctx context.Context) error { return nil }).After("C")
	nB := Node("B", func(ctx context.Context) error { return nil }).After("A")
	nC := Node("C", func(ctx context.Context) error { return nil }).After("B")

	step := DAG(nA, nB, nC)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected transitive cycle error, got nil")
	}
}

func TestDAGUnknownDependency(t *testing.T) {
	nA := Node("A", func(ctx context.Context) error { return nil }).After("nonexistent")

	step := DAG(nA)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected unknown dependency error, got nil")
	}
}

func TestDAGDuplicateNode(t *testing.T) {
	nA1 := Node("A", func(ctx context.Context) error { return nil })
	nA2 := Node("A", func(ctx context.Context) error { return nil })

	step := DAG(nA1, nA2)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected duplicate node error, got nil")
	}
}

func TestDAGDependencyFailure(t *testing.T) {
	errFail := errors.New("upstream failure")
	var downstreamRan atomic.Bool

	n1 := Node("upstream", func(ctx context.Context) error {
		return errFail
	})

	n2 := Node("downstream", func(ctx context.Context) error {
		downstreamRan.Store(true)
		return nil
	}).After("upstream")

	step := DAG(n1, n2)
	err := step(context.Background())
	if !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}
	if downstreamRan.Load() {
		t.Fatal("downstream step should not run when upstream fails")
	}
}

func TestDAGContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	n1 := Node("slow", func(c context.Context) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	n2 := Node("blocked", func(c context.Context) error {
		return nil
	}).After("slow")

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	step := DAG(n1, n2)
	err := step(ctx)
	if err == nil {
		t.Fatal("expected context canceled error, got nil")
	}
}

func TestDAGNilNode(t *testing.T) {
	step := DAG[context.Context](nil)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected nil node error, got nil")
	}
}

func TestDAGEmptyNodeName(t *testing.T) {
	node := Node("", func(ctx context.Context) error { return nil })
	step := DAG(node)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected empty node name error, got nil")
	}
}

func TestDAGNilStep(t *testing.T) {
	node := Node[context.Context]("noop", nil)
	step := DAG(node)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step, got %v", err)
	}
}

func TestDAGPreCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	node := Node("test", func(c context.Context) error {
		return nil
	})

	step := DAG(node)
	err := step(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func BenchmarkDAG(b *testing.B) {
	n1 := Node("a", func(ctx context.Context) error { return nil })
	n2 := Node("b", func(ctx context.Context) error { return nil }).After("a")
	n3 := Node("c", func(ctx context.Context) error { return nil }).After("a")
	n4 := Node("d", func(ctx context.Context) error { return nil }).After("b", "c")

	step := DAG(n1, n2, n3, n4)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = step(ctx)
	}
}
