package flow

import (
	"context"
	"errors"
	"fmt"
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

func TestDAGN(t *testing.T) {
	if err := DAGN[context.Context](2)(context.Background()); err != nil {
		t.Fatalf("expected nil error for empty DAGN, got %v", err)
	}

	n1 := Node("1", func(ctx context.Context) error { return nil })
	n2 := Node("2", func(ctx context.Context) error { return nil })
	if err := DAGN(0, n1, n2)(context.Background()); err != nil {
		t.Fatalf("expected nil error for limit <= 0, got %v", err)
	}

	if err := DAGN(5, n1, n2)(context.Background()); err != nil {
		t.Fatalf("expected nil error for limit >= len, got %v", err)
	}

	var active atomic.Int32
	var maxActive atomic.Int32
	nodes := make([]*DAGNode[context.Context], 8)
	for i := range nodes {
		nodes[i] = Node(fmt.Sprintf("n%d", i), func(ctx context.Context) error {
			cur := active.Add(1)
			for {
				old := maxActive.Load()
				if cur <= old || maxActive.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
			return nil
		})
	}

	step := DAGN(2, nodes...)
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if maxActive.Load() > 2 {
		t.Fatalf("expected max active <= 2, got %d", maxActive.Load())
	}

	errBadDAG := DAGN(2, Node("", func(ctx context.Context) error { return nil }))(context.Background())
	if errBadDAG == nil {
		t.Fatal("expected error on invalid DAG, got nil")
	}

	errFail := errors.New("node failure")
	nFail := Node("fail", func(ctx context.Context) error { return errFail })
	nAfter := Node("after", func(ctx context.Context) error { return nil }).After("fail")
	stepFail := DAGN(1, nFail, nAfter)
	if err := stepFail(context.Background()); !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}

	ctxPreCancel, cancel := context.WithCancel(context.Background())
	cancel()
	nCancel := Node("cancel", func(ctx context.Context) error { return nil })
	if err := DAGN(1, nCancel)(ctxPreCancel); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	ctxSlow, cancelSlow := context.WithCancel(context.Background())
	nSlow := Node("slow", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	nBlocked := Node("blocked", func(ctx context.Context) error {
		return nil
	}).After("slow")
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancelSlow()
	}()
	if err := DAGN(1, nSlow, nBlocked)(ctxSlow); err == nil {
		t.Fatal("expected cancel error, got nil")
	}

	nNilStep := Node[context.Context]("nilStep", nil)
	nOther := Node("other", func(ctx context.Context) error { return nil })
	if err := DAGN(1, nNilStep, nOther)(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil step node in DAGN, got %v", err)
	}

	ctxSemCancel, cancelSem := context.WithCancel(context.Background())
	nBlocker1 := Node("blocker1", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	nBlocker2 := Node("blocker2", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	nBlocker3 := Node("blocker3", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancelSem()
	}()
	if err := DAGN(1, nBlocker1, nBlocker2, nBlocker3)(ctxSemCancel); err == nil {
		t.Fatal("expected cancel error on semaphore acquisition, got nil")
	}

	errDepFail := errors.New("upstream failed in DAGN")
	nUp := Node("up", func(ctx context.Context) error { return errDepFail })
	nDown := Node("down", func(ctx context.Context) error { return nil }).After("up")
	if err := DAGN(1, nUp, nDown)(context.Background()); !errors.Is(err, errDepFail) {
		t.Fatalf("expected %v, got %v", errDepFail, err)
	}

	ctxDepCancel, cancelDep := context.WithCancel(context.Background())
	nSlowDep := Node("slowDep", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	nWaitingDep := Node("waitingDep", func(ctx context.Context) error {
		return nil
	}).After("slowDep")
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancelDep()
	}()
	if err := DAGN(1, nSlowDep, nWaitingDep)(ctxDepCancel); err == nil {
		t.Fatal("expected cancel error while waiting on dependency, got nil")
	}
}

func BenchmarkDAGN(b *testing.B) {
	n1 := Node("a", func(ctx context.Context) error { return nil })
	n2 := Node("b", func(ctx context.Context) error { return nil }).After("a")
	n3 := Node("c", func(ctx context.Context) error { return nil }).After("a")
	n4 := Node("d", func(ctx context.Context) error { return nil }).After("b", "c")

	step := DAGN(2, n1, n2, n3, n4)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = step(ctx)
	}
}

func TestDAGEdgesBasic(t *testing.T) {
	var step1Done, step2Done, step3Done atomic.Bool

	step1 := func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		step1Done.Store(true)
		return nil
	}
	step2 := func(ctx context.Context) error {
		if !step1Done.Load() {
			t.Error("step1 must be done before step2")
		}
		step2Done.Store(true)
		return nil
	}
	step3 := func(ctx context.Context) error {
		if !step2Done.Load() {
			t.Error("step2 must be done before step3")
		}
		step3Done.Store(true)
		return nil
	}

	graph := DAGEdges(
		Edge(step1, step2),
		From(step2).To(step3),
	)

	if err := graph(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if !step1Done.Load() || !step2Done.Load() || !step3Done.Load() {
		t.Fatal("expected all steps to complete")
	}
}

func TestDAGEdgesEmpty(t *testing.T) {
	step := DAGEdges[context.Context]()
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	stepN := DAGEdgesN[context.Context](2)
	if err := stepN(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestDAGEdgesNilStep(t *testing.T) {
	fn := func(ctx context.Context) error { return nil }

	graph1 := DAGEdges(Edge(nil, fn))
	if err := graph1(context.Background()); err == nil {
		t.Fatal("expected error for nil step, got nil")
	}

	graph2 := DAGEdges(Edge(fn, nil))
	if err := graph2(context.Background()); err == nil {
		t.Fatal("expected error for nil step, got nil")
	}

	graph3 := DAGEdgesN(2, Edge(nil, fn))
	if err := graph3(context.Background()); err == nil {
		t.Fatal("expected error for nil step, got nil")
	}
}

func TestDAGEdgesCycle(t *testing.T) {
	fn1 := func(ctx context.Context) error { return nil }
	fn2 := func(ctx context.Context) error { return nil }

	graph := DAGEdges(
		Edge(fn1, fn2),
		Edge(fn2, fn1),
	)

	if err := graph(context.Background()); err == nil {
		t.Fatal("expected cycle error, got nil")
	}
}

func TestDAGEdgesNBounded(t *testing.T) {
	var active atomic.Int64
	var maxActive atomic.Int64

	work := func(ctx context.Context) error {
		cur := active.Add(1)
		for {
			old := maxActive.Load()
			if cur <= old || maxActive.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		active.Add(-1)
		return nil
	}

	root := func(ctx context.Context) error { return nil }
	f1 := func(ctx context.Context) error { return work(ctx) }
	f2 := func(ctx context.Context) error { return work(ctx) }
	f3 := func(ctx context.Context) error { return work(ctx) }
	f4 := func(ctx context.Context) error { return work(ctx) }

	graph := DAGEdgesN(
		2,
		From(root).To(f1, f2, f3, f4),
	)

	if err := graph(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if peak := maxActive.Load(); peak > 2 {
		t.Fatalf("expected max active <= 2, got %d", peak)
	}
}

func BenchmarkDAGEdges(b *testing.B) {
	f1 := func(ctx context.Context) error { return nil }
	f2 := func(ctx context.Context) error { return nil }
	f3 := func(ctx context.Context) error { return nil }
	f4 := func(ctx context.Context) error { return nil }

	graph := DAGEdges(
		Edge(f1, f2, f3),
		Edge(f2, f4),
		Edge(f3, f4),
	)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = graph(ctx)
	}
}

