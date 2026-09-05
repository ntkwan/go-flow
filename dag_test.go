package flow

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	if !errors.Is(err, ErrDAGCycle) {
		t.Fatalf("expected ErrDAGCycle, got %v", err)
	}
	if !strings.Contains(err.Error(), "A -> B -> A") && !strings.Contains(err.Error(), "B -> A -> B") {
		t.Fatalf("expected cycle path in error message, got %q", err.Error())
	}
}

func TestDAGCycleSelf(t *testing.T) {
	nA := Node("A", func(ctx context.Context) error { return nil }).After("A")

	step := DAG(nA)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected self-cycle error, got nil")
	}
	if !errors.Is(err, ErrDAGCycle) {
		t.Fatalf("expected ErrDAGCycle, got %v", err)
	}
	if !strings.Contains(err.Error(), "node A cannot depend on itself") {
		t.Fatalf("expected self-dependency message, got %q", err.Error())
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
	if !errors.Is(err, ErrDAGCycle) {
		t.Fatalf("expected ErrDAGCycle, got %v", err)
	}
	if !strings.Contains(err.Error(), "A -> B -> C -> A") && !strings.Contains(err.Error(), "B -> C -> A -> B") && !strings.Contains(err.Error(), "C -> A -> B -> C") {
		t.Fatalf("expected transitive cycle path in error, got %q", err.Error())
	}
}

func TestDAGCycleComplexBacktracking(t *testing.T) {
	nEntry := Node("entry", func(ctx context.Context) error { return nil }).After("cA")
	nDeadEnd := Node("dead", func(ctx context.Context) error { return nil }).After("entry")
	nCA := Node("cA", func(ctx context.Context) error { return nil }).After("cB")
	nCB := Node("cB", func(ctx context.Context) error { return nil }).After("entry")

	step := DAG(nEntry, nDeadEnd, nCA, nCB)
	err := step(context.Background())
	if !errors.Is(err, ErrDAGCycle) {
		t.Fatalf("expected ErrDAGCycle, got %v", err)
	}
}

func TestDAGCycleBranchBacktracking(t *testing.T) {
	nDead := Node("dead", func(ctx context.Context) error { return nil }).After("c1")
	nC2 := Node("c2", func(ctx context.Context) error { return nil })
	nC1 := Node("c1", func(ctx context.Context) error { return nil }).After("c2")
	nC2.After("c1")

	step := DAG(nC1, nDead, nC2)
	err := step(context.Background())
	if !errors.Is(err, ErrDAGCycle) {
		t.Fatalf("expected ErrDAGCycle, got %v", err)
	}
}

func TestDAGCycleWithResolvedSink(t *testing.T) {
	nSink := Node("sink", func(ctx context.Context) error { return nil }).After("cA")
	nCA := Node("cA", func(ctx context.Context) error { return nil }).After("cB")
	nCB := Node("cB", func(ctx context.Context) error { return nil }).After("cA")

	step := DAG(nSink, nCA, nCB)
	err := step(context.Background())
	if !errors.Is(err, ErrDAGCycle) {
		t.Fatalf("expected ErrDAGCycle, got %v", err)
	}
}

func TestDAGUnknownDependency(t *testing.T) {
	nA := Node("A", func(ctx context.Context) error { return nil }).After("nonexistent")

	step := DAG(nA)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected unknown dependency error, got nil")
	}
	if !errors.Is(err, ErrDAGUnknownDependency) {
		t.Fatalf("expected ErrDAGUnknownDependency, got %v", err)
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
	if !errors.Is(err, ErrDAGDuplicateNode) {
		t.Fatalf("expected ErrDAGDuplicateNode, got %v", err)
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
	if !errors.Is(err, ErrDAGNilNode) {
		t.Fatalf("expected ErrDAGNilNode, got %v", err)
	}
}

func TestDAGEmptyNodeName(t *testing.T) {
	node := Node("", func(ctx context.Context) error { return nil })
	step := DAG(node)
	err := step(context.Background())
	if err == nil {
		t.Fatal("expected empty node name error, got nil")
	}
	if !errors.Is(err, ErrDAGEmptyNodeName) {
		t.Fatalf("expected ErrDAGEmptyNodeName, got %v", err)
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
	blocker1Release := make(chan struct{})
	blocker2Ready := make(chan struct{})
	nBlocker1 := Node("blocker1", func(ctx context.Context) error {
		<-blocker1Release
		return nil
	})
	nBlocker2 := Node("blocker2", func(ctx context.Context) error {
		return nil
	}).When(func(ctx context.Context) bool {
		close(blocker2Ready)
		return true
	})
	go func() {
		<-blocker2Ready
		time.Sleep(5 * time.Millisecond)
		cancelSem()
		close(blocker1Release)
	}()
	if err := DAGN(1, nBlocker1, nBlocker2)(ctxSemCancel); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on semaphore acquisition, got %v", err)
	}

	errDepFail := errors.New("upstream failed in DAGN")
	nUp := Node("up", func(ctx context.Context) error { return errDepFail })
	nDown := Node("down", func(ctx context.Context) error { return nil }).After("up")
	if err := DAGN(1, nUp, nDown)(context.Background()); !errors.Is(err, errDepFail) {
		t.Fatalf("expected %v, got %v", errDepFail, err)
	}

	nSkipped := Node("skipped", func(ctx context.Context) error { return errors.New("should not run") }).When(func(ctx context.Context) bool { return false })
	nAfterSkipped := Node("after_skipped", func(ctx context.Context) error { return nil }).After("skipped")
	if err := DAGN(1, nSkipped, nAfterSkipped)(context.Background()); err != nil {
		t.Fatalf("expected nil error for skipped node in DAGN, got %v", err)
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

	nC1 := Node("c1", func(ctx context.Context) error { return nil }).After("c2")
	nC2 := Node("c2", func(ctx context.Context) error { return nil }).After("c1")
	if err := DAGN(1, nC1, nC2)(context.Background()); err == nil {
		t.Fatal("expected validation error in DAGN, got nil")
	}

	ctxPreCanceled, cancelPre := context.WithCancel(context.Background())
	cancelPre()
	nPre1 := Node("pre1", func(ctx context.Context) error { return nil })
	nPre2 := Node("pre2", func(ctx context.Context) error { return nil })
	if err := DAGN(1, nPre1, nPre2)(ctxPreCanceled); err == nil {
		t.Fatal("expected cancel error with pre-canceled context in DAGN, got nil")
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

func TestDAGToMermaid(t *testing.T) {
	n1 := Node("fetch-user", func(ctx context.Context) error { return nil })
	n2 := Node("fetch-cart", func(ctx context.Context) error { return nil })
	n3 := Node("process-payment", func(ctx context.Context) error { return nil }).After("fetch-user", "fetch-cart")
	n4 := Node("send-receipt", func(ctx context.Context) error { return nil }).After("process-payment")
	n5 := Node("isolated-node", func(ctx context.Context) error { return nil })
	n6 := Node("123step", func(ctx context.Context) error { return nil })

	mermaid, err := DAGToMermaid(n1, n2, n3, n4, n5, n6)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if !strings.HasPrefix(mermaid, "graph TD") {
		t.Fatalf("expected mermaid to start with 'graph TD', got %q", mermaid)
	}
	if !strings.Contains(mermaid, "fetch_user[\"fetch-user\"] --> process_payment[\"process-payment\"]") {
		t.Fatalf("missing fetch-user -> process-payment edge: %s", mermaid)
	}
	if !strings.Contains(mermaid, "fetch_cart[\"fetch-cart\"] --> process_payment[\"process-payment\"]") {
		t.Fatalf("missing fetch-cart -> process-payment edge: %s", mermaid)
	}
	if !strings.Contains(mermaid, "process_payment[\"process-payment\"] --> send_receipt[\"send-receipt\"]") {
		t.Fatalf("missing process-payment -> send-receipt edge: %s", mermaid)
	}
	if !strings.Contains(mermaid, "isolated_node[\"isolated-node\"]") {
		t.Fatalf("missing isolated node: %s", mermaid)
	}
	if !strings.Contains(mermaid, "n_123step[\"123step\"]") {
		t.Fatalf("missing digit-prefixed node: %s", mermaid)
	}
	if escapeMermaidID("") != "node" {
		t.Fatalf("expected 'node' for empty string, got %q", escapeMermaidID(""))
	}
	if escapeMermaidID("a_z_A_Z_0_9") != "a_z_A_Z_0_9" {
		t.Fatalf("expected 'a_z_A_Z_0_9', got %q", escapeMermaidID("a_z_A_Z_0_9"))
	}
	if escapeMermaidID("0a") != "n_0a" {
		t.Fatalf("expected 'n_0a', got %q", escapeMermaidID("0a"))
	}
	if escapeMermaidID("9z") != "n_9z" {
		t.Fatalf("expected 'n_9z', got %q", escapeMermaidID("9z"))
	}
	if escapeMermaidID("@#$") != "___" {
		t.Fatalf("expected '___', got %q", escapeMermaidID("@#$"))
	}
}

func TestDAGToMermaidEmptyAndErrors(t *testing.T) {
	out, err := DAGToMermaid[context.Context]()
	if err != nil || out != "graph TD" {
		t.Fatalf("expected 'graph TD', got %q (err=%v)", out, err)
	}

	n1 := Node("cycle1", func(ctx context.Context) error { return nil }).After("cycle2")
	n2 := Node("cycle2", func(ctx context.Context) error { return nil }).After("cycle1")
	_, err = DAGToMermaid(n1, n2)
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}
}

func TestDAGToDOT(t *testing.T) {
	n1 := Node("stepA", func(ctx context.Context) error { return nil })
	n2 := Node("stepB", func(ctx context.Context) error { return nil }).After("stepA")
	n3 := Node("stepC", func(ctx context.Context) error { return nil })

	dot, err := DAGToDOT(n1, n2, n3)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if !strings.HasPrefix(dot, "digraph DAG {") || !strings.HasSuffix(dot, "}") {
		t.Fatalf("invalid DOT structure: %s", dot)
	}
	if !strings.Contains(dot, "\"stepA\" -> \"stepB\";") {
		t.Fatalf("missing edge in DOT: %s", dot)
	}
	if !strings.Contains(dot, "\"stepC\";") {
		t.Fatalf("missing isolated node in DOT: %s", dot)
	}

	outEmpty, err := DAGToDOT[context.Context]()
	if err != nil || outEmpty != "digraph DAG {\n}" {
		t.Fatalf("expected empty DOT, got %q (err=%v)", outEmpty, err)
	}

	_, err = DAGToDOT(Node[context.Context]("a", nil).After("b"))
	if err == nil {
		t.Fatal("expected error for missing dep, got nil")
	}
}

func TestDAGPlan(t *testing.T) {
	var trace []string
	var mu sync.Mutex
	record := func(name string) Step[context.Context] {
		return func(ctx context.Context) error {
			mu.Lock()
			trace = append(trace, name)
			mu.Unlock()
			return nil
		}
	}

	n1 := Node("n1", record("n1"))
	n2 := Node("n2", record("n2")).After("n1")

	plan := NewDAG(n1, n2)
	if err := plan.Validate(); err != nil {
		t.Fatalf("expected nil error from plan.Validate(), got %v", err)
	}

	m, err := plan.ToMermaid()
	if err != nil || !strings.Contains(m, "n1[\"n1\"] --> n2[\"n2\"]") {
		t.Fatalf("unexpected mermaid: %q (err=%v)", m, err)
	}

	d, err := plan.ToDOT()
	if err != nil || !strings.Contains(d, "\"n1\" -> \"n2\";") {
		t.Fatalf("unexpected DOT: %q (err=%v)", d, err)
	}

	step := plan.Step()
	if err := step(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(trace) != 2 || trace[0] != "n1" || trace[1] != "n2" {
		t.Fatalf("unexpected trace: %v", trace)
	}

	trace = nil
	stepN := plan.StepN(1)
	if err := stepN(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(trace) != 2 || trace[0] != "n1" || trace[1] != "n2" {
		t.Fatalf("unexpected trace: %v", trace)
	}

	cyclicPlan := NewDAG(
		Node("cA", func(ctx context.Context) error { return nil }).After("cB"),
		Node("cB", func(ctx context.Context) error { return nil }).After("cA"),
	)
	if err := cyclicPlan.Validate(); !errors.Is(err, ErrDAGCycle) {
		t.Fatalf("expected ErrDAGCycle from cyclicPlan.Validate(), got %v", err)
	}

	var nilPlan *DAGPlan[context.Context]
	if err := nilPlan.Validate(); err != nil {
		t.Fatalf("expected nil error for nilPlan.Validate(), got %v", err)
	}
	if err := nilPlan.Step()(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil plan step, got %v", err)
	}
	if err := nilPlan.StepN(1)(context.Background()); err != nil {
		t.Fatalf("expected nil error for nil plan stepN, got %v", err)
	}
	if out, err := nilPlan.ToMermaid(); err != nil || out != "graph TD" {
		t.Fatalf("expected 'graph TD', got %q", out)
	}
	if out, err := nilPlan.ToDOT(); err != nil || out != "digraph DAG {\n}" {
		t.Fatalf("expected empty DOT, got %q", out)
	}
}

func TestDAGEdgesExport(t *testing.T) {
	f1 := func(ctx context.Context) error { return nil }
	f2 := func(ctx context.Context) error { return nil }

	m, err := DAGEdgesToMermaid(Edge(f1, f2))
	if err != nil || !strings.HasPrefix(m, "graph TD") {
		t.Fatalf("unexpected edges mermaid: %q (err=%v)", m, err)
	}

	d, err := DAGEdgesToDOT(Edge(f1, f2))
	if err != nil || !strings.HasPrefix(d, "digraph DAG {") {
		t.Fatalf("unexpected edges DOT: %q (err=%v)", d, err)
	}

	mEmpty, err := DAGEdgesToMermaid[context.Context]()
	if err != nil || mEmpty != "graph TD" {
		t.Fatalf("expected empty edges mermaid, got %q", mEmpty)
	}

	dEmpty, err := DAGEdgesToDOT[context.Context]()
	if err != nil || dEmpty != "digraph DAG {\n}" {
		t.Fatalf("expected empty edges DOT, got %q", dEmpty)
	}

	_, err = DAGEdgesToMermaid(Edge[context.Context](nil, nil))
	if err == nil {
		t.Fatal("expected error for nil edge, got nil")
	}

	_, err = DAGEdgesToDOT(Edge[context.Context](nil, nil))
	if err == nil {
		t.Fatal("expected error for nil edge, got nil")
	}

	plan, err := NewDAGEdges(Edge(f1, f2))
	if err != nil || plan == nil {
		t.Fatalf("expected valid plan from edges, got err=%v", err)
	}

	_, err = NewDAGEdges(Edge[context.Context](nil, nil))
	if err == nil {
		t.Fatal("expected error from NewDAGEdges with nil edge, got nil")
	}
}

func TestDAGEventDrivenMultiLevel(t *testing.T) {
	var mu sync.Mutex
	var order []string

	makeNode := func(name string, after ...string) *DAGNode[context.Context] {
		return Node(name, func(ctx context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}).After(after...)
	}

	nA := makeNode("A")
	nB := makeNode("B", "A")
	nC := makeNode("C", "A")
	nD := makeNode("D", "B", "C")
	nE := makeNode("E", "D")

	dag := DAG(nE, nD, nC, nB, nA)
	if err := dag(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if len(order) != 5 || order[0] != "A" || order[4] != "E" {
		t.Fatalf("unexpected execution order: %v", order)
	}
}

func TestDAGFailureIsolationInDegree(t *testing.T) {
	var executedD atomic.Bool
	errFail := errors.New("node B failed")

	nA := Node("A", func(ctx context.Context) error { return nil })
	nB := Node("B", func(ctx context.Context) error { return errFail }).After("A")
	nC := Node("C", func(ctx context.Context) error { return nil }).After("A")
	nD := Node("D", func(ctx context.Context) error {
		executedD.Store(true)
		return nil
	}).After("B", "C")

	dag := DAG(nA, nB, nC, nD)
	err := dag(context.Background())
	if !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}

	if executedD.Load() {
		t.Fatal("node D should not execute when dependency B fails")
	}
}

func TestDAGConcurrentFailureStart(t *testing.T) {
	errFail := errors.New("fast failure")
	nodes := make([]*DAGNode[context.Context], 30)
	nodes[0] = Node("fail", func(ctx context.Context) error {
		return errFail
	})
	for i := 1; i < 30; i++ {
		nodes[i] = Node(fmt.Sprintf("node-%d", i), func(ctx context.Context) error {
			time.Sleep(5 * time.Millisecond)
			return nil
		})
	}

	dag := DAG(nodes...)
	err := dag(context.Background())
	if !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}
}

func TestDAGNContentionFailure(t *testing.T) {
	errFail := errors.New("contended failure")
	n1Hold := make(chan struct{})
	n2Waiting := make(chan struct{})
	n1 := Node("n1", func(ctx context.Context) error {
		<-n1Hold
		return errFail
	})
	n2 := Node("n2", func(ctx context.Context) error {
		return nil
	}).When(func(ctx context.Context) bool {
		close(n2Waiting)
		return true
	})

	go func() {
		<-n2Waiting
		time.Sleep(5 * time.Millisecond)
		close(n1Hold)
	}()

	dagn := DAGN(1, n1, n2)
	err := dagn(context.Background())
	if !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}

	nodes := make([]*DAGNode[context.Context], 25)
	nodes[0] = Node("fail", func(ctx context.Context) error {
		return errFail
	})
	for i := 1; i < 25; i++ {
		nodes[i] = Node(fmt.Sprintf("n-%d", i), func(ctx context.Context) error {
			time.Sleep(5 * time.Millisecond)
			return nil
		})
	}
	dagnMany := DAGN(2, nodes...)
	if err := dagnMany(context.Background()); !errors.Is(err, errFail) {
		t.Fatalf("expected %v, got %v", errFail, err)
	}

	ctxLateCancel, cancelLate := context.WithCancel(context.Background())
	nLate1 := Node("late1", func(ctx context.Context) error { return nil })
	nLate2 := Node("late2", func(ctx context.Context) error {
		cancelLate()
		return nil
	}).After("late1")
	if err := DAGN(1, nLate1, nLate2)(ctxLateCancel); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on late cancel in DAGN, got %v", err)
	}

	ctxLateDAG, cancelLateDAG := context.WithCancel(context.Background())
	nLateDAG := Node("lateDAG", func(ctx context.Context) error {
		cancelLateDAG()
		return nil
	})
	if err := DAG(nLateDAG)(ctxLateDAG); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on late cancel in DAG, got %v", err)
	}
}

func BenchmarkDAGToMermaid(b *testing.B) {
	n1 := Node("fetch-user", func(ctx context.Context) error { return nil })
	n2 := Node("fetch-cart", func(ctx context.Context) error { return nil })
	n3 := Node("process-payment", func(ctx context.Context) error { return nil }).After("fetch-user", "fetch-cart")
	n4 := Node("send-receipt", func(ctx context.Context) error { return nil }).After("process-payment")

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = DAGToMermaid(n1, n2, n3, n4)
	}
}
