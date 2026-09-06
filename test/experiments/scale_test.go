package experiments_test

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntkwan/go-flow"
)

func TestScaleWideDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale test in short mode")
	}

	const nodeCount = 10000
	var executed atomic.Int32

	root := flow.Node("root", func(ctx context.Context) error { return nil })
	sink := flow.Node("sink", func(ctx context.Context) error { return nil })

	nodes := make([]*flow.DAGNode[context.Context], 0, nodeCount+2)
	nodes = append(nodes, root)

	sinkDeps := make([]string, nodeCount)
	for i := range nodeCount {
		name := fmt.Sprintf("worker-%d", i)
		sinkDeps[i] = name
		node := flow.Node(name, func(ctx context.Context) error {
			executed.Add(1)
			return nil
		}).After("root")
		nodes = append(nodes, node)
	}

	sink.After(sinkDeps...)
	nodes = append(nodes, sink)

	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	startTime := time.Now()
	dag := flow.DAG(nodes...)
	compileDuration := time.Since(startTime)

	execStart := time.Now()
	if err := dag(context.Background()); err != nil {
		t.Fatalf("expected nil error on wide dag, got %v", err)
	}
	execDuration := time.Since(execStart)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	if executed.Load() != nodeCount {
		t.Fatalf("expected %d nodes executed, got %d", nodeCount, executed.Load())
	}

	allocBytes := memAfter.TotalAlloc - memBefore.TotalAlloc
	bytesPerNode := float64(allocBytes) / float64(nodeCount)

	t.Logf("Wide DAG (10,000 nodes): Compile=%v, Exec=%v, TotalAlloc=%d bytes (%.1f bytes/node)",
		compileDuration, execDuration, allocBytes, bytesPerNode)
}

func TestScaleDeepDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale test in short mode")
	}

	const chainLength = 5000
	var executed atomic.Int32

	nodes := make([]*flow.DAGNode[context.Context], chainLength)
	for i := range chainLength {
		name := fmt.Sprintf("chain-%d", i)
		node := flow.Node(name, func(ctx context.Context) error {
			executed.Add(1)
			return nil
		})
		if i > 0 {
			node.After(fmt.Sprintf("chain-%d", i-1))
		}
		nodes[i] = node
	}

	startTime := time.Now()
	dag := flow.DAG(nodes...)
	compileDuration := time.Since(startTime)

	execStart := time.Now()
	if err := dag(context.Background()); err != nil {
		t.Fatalf("expected nil error on deep dag, got %v", err)
	}
	execDuration := time.Since(execStart)

	if executed.Load() != chainLength {
		t.Fatalf("expected %d nodes executed, got %d", chainLength, executed.Load())
	}

	t.Logf("Deep DAG (5,000 linear nodes): Compile=%v, Exec=%v", compileDuration, execDuration)
}

func TestScaleDiamondLattice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale test in short mode")
	}

	const layers = 1000
	var executed atomic.Int32

	nodes := make([]*flow.DAGNode[context.Context], 0, layers*3+1)
	root := flow.Node("layer-0-root", func(ctx context.Context) error {
		executed.Add(1)
		return nil
	})
	nodes = append(nodes, root)

	prevSink := "layer-0-root"
	for l := 1; l <= layers; l++ {
		left := fmt.Sprintf("layer-%d-left", l)
		right := fmt.Sprintf("layer-%d-right", l)
		sink := fmt.Sprintf("layer-%d-sink", l)

		nodeLeft := flow.Node(left, func(ctx context.Context) error {
			executed.Add(1)
			return nil
		}).After(prevSink)

		nodeRight := flow.Node(right, func(ctx context.Context) error {
			executed.Add(1)
			return nil
		}).After(prevSink)

		nodeSink := flow.Node(sink, func(ctx context.Context) error {
			executed.Add(1)
			return nil
		}).After(left, right)

		nodes = append(nodes, nodeLeft, nodeRight, nodeSink)
		prevSink = sink
	}

	startTime := time.Now()
	dag := flow.DAG(nodes...)
	compileDuration := time.Since(startTime)

	execStart := time.Now()
	if err := dag(context.Background()); err != nil {
		t.Fatalf("expected nil error on diamond lattice, got %v", err)
	}
	execDuration := time.Since(execStart)

	expectedCount := int32(1 + layers*3)
	if executed.Load() != expectedCount {
		t.Fatalf("expected %d nodes executed, got %d", expectedCount, executed.Load())
	}

	t.Logf("Diamond Lattice (%d layers, %d nodes): Compile=%v, Exec=%v", layers, expectedCount, compileDuration, execDuration)
}

func BenchmarkScaleWide10k(b *testing.B) {
	const nodeCount = 10000
	root := flow.Node("root", func(ctx context.Context) error { return nil })
	sink := flow.Node("sink", func(ctx context.Context) error { return nil })

	nodes := make([]*flow.DAGNode[context.Context], 0, nodeCount+2)
	nodes = append(nodes, root)
	sinkDeps := make([]string, nodeCount)
	for i := range nodeCount {
		name := fmt.Sprintf("w-%d", i)
		sinkDeps[i] = name
		node := flow.Node(name, func(ctx context.Context) error { return nil }).After("root")
		nodes = append(nodes, node)
	}
	sink.After(sinkDeps...)
	nodes = append(nodes, sink)

	dag := flow.DAG(nodes...)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = dag(ctx)
	}
}

func BenchmarkScaleDeep5k(b *testing.B) {
	const chainLength = 5000
	nodes := make([]*flow.DAGNode[context.Context], chainLength)
	for i := range chainLength {
		name := fmt.Sprintf("c-%d", i)
		node := flow.Node(name, func(ctx context.Context) error { return nil })
		if i > 0 {
			node.After(fmt.Sprintf("c-%d", i-1))
		}
		nodes[i] = node
	}

	dag := flow.DAG(nodes...)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = dag(ctx)
	}
}
