package flow_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntkwan/go-flow"
)

func TestDAGConditionalWhenUnless(t *testing.T) {
	type testContext struct {
		context.Context
		shouldRun bool
		ranA      atomic.Bool
		ranB      atomic.Bool
		ranC      atomic.Bool
	}

	stepA := func(ctx *testContext) error {
		ctx.ranA.Store(true)
		return nil
	}
	stepB := func(ctx *testContext) error {
		ctx.ranB.Store(true)
		return nil
	}
	stepC := func(ctx *testContext) error {
		ctx.ranC.Store(true)
		return nil
	}

	t.Run("When true executes and When false skips", func(t *testing.T) {
		dag := flow.DAG(
			flow.Node("a", stepA).When(func(ctx *testContext) bool { return ctx.shouldRun }),
			flow.Node("b", stepB).After("a").When(func(ctx *testContext) bool { return true }),
		)

		ctxFalse := &testContext{Context: context.Background(), shouldRun: false}
		if err := dag(ctxFalse); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ctxFalse.ranA.Load() {
			t.Error("expected node A to be skipped")
		}
		if !ctxFalse.ranB.Load() {
			t.Error("expected node B to execute after skipped node A")
		}

		ctxTrue := &testContext{Context: context.Background(), shouldRun: true}
		if err := dag(ctxTrue); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ctxTrue.ranA.Load() {
			t.Error("expected node A to run")
		}
		if !ctxTrue.ranB.Load() {
			t.Error("expected node B to run")
		}
	})

	t.Run("Unless skips when true and executes when false", func(t *testing.T) {
		dag := flow.DAGN(2,
			flow.Node("a", stepA).Unless(func(ctx *testContext) bool { return ctx.shouldRun }),
			flow.Node("c", stepC).After("a").Unless(func(ctx *testContext) bool { return false }),
		)

		ctxShouldSkip := &testContext{Context: context.Background(), shouldRun: true}
		if err := dag(ctxShouldSkip); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ctxShouldSkip.ranA.Load() {
			t.Error("expected node A to be skipped with Unless(true)")
		}
		if !ctxShouldSkip.ranC.Load() {
			t.Error("expected node C to run with Unless(false)")
		}
	})

	t.Run("Nil predicates are ignored", func(t *testing.T) {
		node := flow.Node("a", stepA).When(nil).Unless(nil)
		ctx := &testContext{Context: context.Background()}
		dag := flow.DAG(node)
		if err := dag(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ctx.ranA.Load() {
			t.Error("expected node A to execute when nil predicates supplied")
		}
	})
}

func TestDAGNodeFluentDecorators(t *testing.T) {
	type customContext struct {
		context.Context
		attempts int
	}

	t.Run("WithTimeout", func(t *testing.T) {
		slowStep := func(ctx *customContext) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		}
		node := flow.Node("slow", slowStep).WithTimeout(10 * time.Millisecond)
		dag := flow.DAG(node)
		ctx := &customContext{Context: context.Background()}
		err := dag(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline exceeded, got: %v", err)
		}
	})

	t.Run("WithRetry", func(t *testing.T) {
		flakyStep := func(ctx *customContext) error {
			ctx.attempts++
			if ctx.attempts < 3 {
				return errors.New("transient")
			}
			return nil
		}
		node := flow.Node("flaky", flakyStep).WithRetry(3, 1*time.Millisecond)
		dag := flow.DAG(node)
		ctx := &customContext{Context: context.Background()}
		if err := dag(ctx); err != nil {
			t.Fatalf("expected success on retry, got: %v", err)
		}
		if ctx.attempts != 3 {
			t.Fatalf("expected 3 attempts, got %d", ctx.attempts)
		}
	})

	t.Run("WithRecover", func(t *testing.T) {
		panickyStep := func(ctx *customContext) error {
			panic("boom")
		}
		node := flow.Node("panicky", panickyStep).WithRecover()
		dag := flow.DAG(node)
		ctx := &customContext{Context: context.Background()}
		err := dag(ctx)
		if err == nil || !strings.Contains(err.Error(), "panic recovered") {
			t.Fatalf("expected panic error, got: %v", err)
		}
	})

	t.Run("WithCatch", func(t *testing.T) {
		failingStep := func(ctx *customContext) error {
			return errors.New("raw error")
		}
		node := flow.Node("failing", failingStep).WithCatch(func(ctx *customContext, err error) error {
			return errors.New("handled error")
		})
		dag := flow.DAG(node)
		ctx := &customContext{Context: context.Background()}
		err := dag(ctx)
		if err == nil || err.Error() != "handled error" {
			t.Fatalf("expected handled error, got: %v", err)
		}
	})

	t.Run("WithFallback", func(t *testing.T) {
		failingStep := func(ctx *customContext) error {
			return errors.New("primary failure")
		}
		fallbackStep := func(ctx *customContext) error {
			ctx.attempts = 42
			return nil
		}
		node := flow.Node("fallback", failingStep).WithFallback(fallbackStep)
		dag := flow.DAG(node)
		ctx := &customContext{Context: context.Background()}
		if err := dag(ctx); err != nil {
			t.Fatalf("expected fallback success, got: %v", err)
		}
		if ctx.attempts != 42 {
			t.Fatalf("expected fallback attempt, got: %d", ctx.attempts)
		}
	})

	t.Run("Nil steps do not panic", func(t *testing.T) {
		node := flow.Node[context.Context]("nil", nil).
			WithTimeout(time.Second).
			WithRetry(2).
			WithRecover().
			WithCatch(nil).
			WithFallback(nil)
		dag := flow.DAG(node)
		if err := dag(context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestDAGReport(t *testing.T) {
	stepA := func(ctx context.Context) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	}
	stepB := func(ctx context.Context) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	}
	stepC := func(ctx context.Context) error {
		return errors.New("step c failure")
	}

	t.Run("DAGWithReport success and query methods", func(t *testing.T) {
		exec := flow.DAGWithReport(
			flow.Node("a", stepA),
			flow.Node("b", stepB).After("a"),
			flow.Node("skipped", stepA).After("a").When(func(ctx context.Context) bool { return false }),
		)

		report, err := exec(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if report == nil {
			t.Fatal("expected non-nil report")
		}
		if report.Duration <= 0 {
			t.Errorf("expected positive duration, got: %v", report.Duration)
		}
		if len(report.Nodes) != 3 {
			t.Fatalf("expected 3 node reports, got: %d", len(report.Nodes))
		}

		nodeA := report.Node("a")
		if nodeA == nil || nodeA.Status != flow.NodeStatusSuccess {
			t.Fatalf("expected node A success, got: %+v", nodeA)
		}
		if report.Node("unknown") != nil {
			t.Error("expected nil for unknown node")
		}

		successes := report.Successful()
		if len(successes) != 2 {
			t.Fatalf("expected 2 successful nodes, got: %d", len(successes))
		}

		skipped := report.Skipped()
		if len(skipped) != 1 || skipped[0].Name != "skipped" {
			t.Fatalf("expected 1 skipped node, got: %+v", skipped)
		}

		failed := report.Failed()
		if len(failed) != 0 {
			t.Fatalf("expected 0 failed nodes, got: %d", len(failed))
		}
	})

	t.Run("DAGWithReport failure and errors", func(t *testing.T) {
		exec := flow.DAGWithReport(
			flow.Node("a", stepA),
			flow.Node("c", stepC).After("a"),
			flow.Node("d", stepB).After("c"),
		)

		report, err := exec(context.Background())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if report == nil {
			t.Fatal("expected report even on failure")
		}
		if report.Node("c").Status != flow.NodeStatusFailed {
			t.Fatalf("expected node c failed, got: %v", report.Node("c").Status)
		}
		if len(report.Failed()) != 1 {
			t.Fatalf("expected 1 failed node, got: %d", len(report.Failed()))
		}
	})

	t.Run("DAGWithReport context canceled during execution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var entered atomic.Bool
		blockingStep := func(ctx context.Context) error {
			entered.Store(true)
			<-ctx.Done()
			return ctx.Err()
		}
		waitingStep := func(ctx context.Context) error {
			return nil
		}

		exec := flow.DAGWithReport(
			flow.Node("block", blockingStep),
			flow.Node("wait", waitingStep).After("block"),
		)

		go func() {
			for !entered.Load() {
				time.Sleep(1 * time.Millisecond)
			}
			cancel()
		}()

		report, err := exec(ctx)
		if err == nil {
			t.Fatal("expected context canceled error")
		}
		if report == nil {
			t.Fatal("expected non-nil report on context cancellation")
		}
	})

	t.Run("DAGWithReport pre-canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		exec := flow.DAGWithReport(
			flow.Node("a", stepA),
		)
		report, err := exec(ctx)
		if err == nil {
			t.Fatal("expected pre-canceled error")
		}
		if report.Node("a").Status != flow.NodeStatusFailed {
			t.Errorf("expected failed status on pre-canceled context")
		}
	})

	t.Run("DAGWithReport empty and compile error", func(t *testing.T) {
		emptyExec := flow.DAGWithReport[context.Context]()
		rep, err := emptyExec(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected result on empty nodes: rep=%v, err=%v", rep, err)
		}

		errExec := flow.DAGWithReport(flow.Node[context.Context]("", nil))
		rep, err = errExec(context.Background())
		if err == nil || rep != nil {
			t.Fatalf("expected compile error on empty name: rep=%v, err=%v", rep, err)
		}
	})

	t.Run("DAGNWithReport bounded execution and nil steps", func(t *testing.T) {
		exec := flow.DAGNWithReport(2,
			flow.Node[context.Context]("nil_step", nil),
			flow.Node("a", stepA),
			flow.Node("b", stepB),
			flow.Node("skipped", stepA).When(func(ctx context.Context) bool { return false }),
		)

		report, err := exec(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(report.Nodes) != 4 {
			t.Fatalf("expected 4 reports, got: %d", len(report.Nodes))
		}
		if report.Node("nil_step").Status != flow.NodeStatusSuccess {
			t.Fatalf("expected success for nil step, got: %v", report.Node("nil_step").Status)
		}
		if report.Node("skipped").Status != flow.NodeStatusSkipped {
			t.Fatalf("expected skipped status, got: %v", report.Node("skipped").Status)
		}
	})

	t.Run("DAGNWithReport limit fallback and errors", func(t *testing.T) {
		fallbackExec := flow.DAGNWithReport(0, flow.Node("a", stepA))
		rep, err := fallbackExec(context.Background())
		if err != nil || len(rep.Nodes) != 1 {
			t.Fatalf("unexpected fallback: rep=%v, err=%v", rep, err)
		}

		errExec := flow.DAGNWithReport(1, flow.Node[context.Context]("", nil), flow.Node[context.Context]("b", nil))
		rep, err = errExec(context.Background())
		if err == nil || rep != nil {
			t.Fatalf("expected compile error: rep=%v, err=%v", rep, err)
		}
	})

	t.Run("DAGNWithReport pre-canceled and failure propagation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		exec := flow.DAGNWithReport(1,
			flow.Node("a", stepA),
			flow.Node("b", stepB),
		)
		rep, err := exec(ctx)
		if err == nil || rep.Node("a").Status != flow.NodeStatusFailed {
			t.Fatalf("expected failure on canceled ctx: rep=%v, err=%v", rep, err)
		}

		failExec := flow.DAGNWithReport(1,
			flow.Node("c", stepC),
			flow.Node("after_c", stepA).After("c"),
		)
		rep, err = failExec(context.Background())
		if err == nil || rep.Node("c").Status != flow.NodeStatusFailed {
			t.Fatalf("expected failure report on c: rep=%v, err=%v", rep, err)
		}

		ctxC, cancelC := context.WithCancel(context.Background())
		var blockEntered atomic.Bool
		blockStep := func(ctx context.Context) error {
			blockEntered.Store(true)
			<-ctx.Done()
			return ctx.Err()
		}
		cancelingExec := flow.DAGNWithReport(1,
			flow.Node("block", blockStep),
			flow.Node("after", stepA).After("block"),
		)
		go func() {
			for !blockEntered.Load() {
				time.Sleep(1 * time.Millisecond)
			}
			cancelC()
		}()
		rep, err = cancelingExec(ctxC)
		if err == nil {
			t.Fatalf("expected canceled error, got nil: %v", rep)
		}

		ctxSem, cancelSem := context.WithCancel(context.Background())
		var semStarted atomic.Int32
		semBlockStep := func(ctx context.Context) error {
			semStarted.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}
		semExec := flow.DAGNWithReport(1,
			flow.Node("b1", semBlockStep),
			flow.Node("b2", semBlockStep),
			flow.Node("b3", semBlockStep),
		)
		go func() {
			for semStarted.Load() < 1 {
				time.Sleep(1 * time.Millisecond)
			}
			time.Sleep(10 * time.Millisecond)
			cancelSem()
		}()
		rep, err = semExec(ctxSem)
		if err == nil {
			t.Fatalf("expected error on sem canceled: rep=%v", rep)
		}

		ctxDep, cancelDep := context.WithCancel(context.Background())
		var depEntered atomic.Bool
		slowDepStep := func(ctx context.Context) error {
			depEntered.Store(true)
			time.Sleep(50 * time.Millisecond)
			return nil
		}
		depNode := flow.Node("slow_parent", slowDepStep)
		childNode := flow.Node("child", stepA).After("slow_parent")
		extraNode := flow.Node("extra", stepB)
		depExec := flow.DAGNWithReport(1, depNode, childNode, extraNode)
		go func() {
			for !depEntered.Load() {
				time.Sleep(1 * time.Millisecond)
			}
			cancelDep()
		}()
		rep, err = depExec(ctxDep)
		if err == nil {
			t.Fatalf("expected error on dep canceled, got nil: %v", rep)
		}

		ctxAfterDep, cancelAfterDep := context.WithCancel(context.Background())
		parentStep := func(ctx context.Context) error {
			cancelAfterDep()
			return nil
		}
		pNode := flow.Node("p", parentStep)
		cNode := flow.Node("c", stepA).After("p")
		eNode := flow.Node("e", stepB)
		execAfterDep := flow.DAGNWithReport(1, pNode, cNode, eNode)
		rep, err = execAfterDep(ctxAfterDep)
		if err == nil {
			t.Fatalf("expected error on after-dep canceled, got nil: %v", rep)
		}
	})

	t.Run("DAGEdgesWithReport and DAGEdgesNWithReport", func(t *testing.T) {
		step1 := func(ctx context.Context) error { return nil }
		step2 := func(ctx context.Context) error { return nil }

		edgesExec := flow.DAGEdgesWithReport(
			flow.Edge(step1, step2),
		)
		rep, err := edgesExec(context.Background())
		if err != nil || len(rep.Nodes) != 2 {
			t.Fatalf("unexpected edges report: rep=%v, err=%v", rep, err)
		}

		edgesNExec := flow.DAGEdgesNWithReport(1,
			flow.Edge(step1, step2),
		)
		rep, err = edgesNExec(context.Background())
		if err != nil || len(rep.Nodes) != 2 {
			t.Fatalf("unexpected edgesN report: rep=%v, err=%v", rep, err)
		}

		emptyEdges := flow.DAGEdgesWithReport[context.Context]()
		rep, err = emptyEdges(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected empty edges report: rep=%v, err=%v", rep, err)
		}

		emptyEdgesN := flow.DAGEdgesNWithReport[context.Context](1)
		rep, err = emptyEdgesN(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected empty edgesN report: rep=%v, err=%v", rep, err)
		}

		nilEdges := flow.DAGEdgesWithReport(flow.Edge[context.Context](nil, step2))
		rep, err = nilEdges(context.Background())
		if err == nil || rep != nil {
			t.Fatalf("expected nil edge step error, got: %v", err)
		}

		nilEdgesN := flow.DAGEdgesNWithReport(1, flow.Edge[context.Context](nil, step2))
		rep, err = nilEdgesN(context.Background())
		if err == nil || rep != nil {
			t.Fatalf("expected nil edge step error, got: %v", err)
		}
	})

	t.Run("DAGPlan report methods", func(t *testing.T) {
		plan := flow.NewDAG(
			flow.Node("a", stepA),
			flow.Node("b", stepB).After("a"),
		)

		rep, err := plan.ExecWithReport(context.Background())
		if err != nil || len(rep.Nodes) != 2 {
			t.Fatalf("unexpected ExecWithReport: rep=%v, err=%v", rep, err)
		}

		rep, err = plan.ExecNWithReport(1, context.Background())
		if err != nil || len(rep.Nodes) != 2 {
			t.Fatalf("unexpected ExecNWithReport: rep=%v, err=%v", rep, err)
		}

		stepRepFn := plan.StepWithReport()
		rep, err = stepRepFn(context.Background())
		if err != nil || len(rep.Nodes) != 2 {
			t.Fatalf("unexpected StepWithReport: rep=%v, err=%v", rep, err)
		}

		stepNRepFn := plan.StepNWithReport(1)
		rep, err = stepNRepFn(context.Background())
		if err != nil || len(rep.Nodes) != 2 {
			t.Fatalf("unexpected StepNWithReport: rep=%v, err=%v", rep, err)
		}

		var nilPlan *flow.DAGPlan[context.Context]
		rep, err = nilPlan.ExecWithReport(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected nilPlan ExecWithReport: rep=%v, err=%v", rep, err)
		}
		rep, err = nilPlan.ExecNWithReport(1, context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected nilPlan ExecNWithReport: rep=%v, err=%v", rep, err)
		}
		rep, err = nilPlan.StepWithReport()(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected nilPlan StepWithReport: rep=%v, err=%v", rep, err)
		}
		rep, err = nilPlan.StepNWithReport(1)(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected nilPlan StepNWithReport: rep=%v, err=%v", rep, err)
		}

		emptyPlan := flow.NewDAG[context.Context]()
		rep, err = emptyPlan.ExecWithReport(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected emptyPlan ExecWithReport: rep=%v, err=%v", rep, err)
		}
		rep, err = emptyPlan.ExecNWithReport(1, context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected emptyPlan ExecNWithReport: rep=%v, err=%v", rep, err)
		}
		rep, err = emptyPlan.StepWithReport()(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected emptyPlan StepWithReport: rep=%v, err=%v", rep, err)
		}
		rep, err = emptyPlan.StepNWithReport(1)(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected emptyPlan StepNWithReport: rep=%v, err=%v", rep, err)
		}
	})

	t.Run("Nil DAGReport receiver methods", func(t *testing.T) {
		var nilRep *flow.DAGReport
		if nilRep.Node("a") != nil {
			t.Error("expected nil node on nil report")
		}
		if nilRep.Successful() != nil {
			t.Error("expected nil successful on nil report")
		}
		if nilRep.Failed() != nil {
			t.Error("expected nil failed on nil report")
		}
		if nilRep.Skipped() != nil {
			t.Error("expected nil skipped on nil report")
		}
	})

	t.Run("DAGWithReport and DAGNWithReport skipped dependents", func(t *testing.T) {
		nA := flow.Node("a", func(ctx context.Context) error { return nil }).When(func(ctx context.Context) bool { return false })
		nB := flow.Node("b", func(ctx context.Context) error { return nil }).After("a")
		rep, err := flow.DAGWithReport(nA, nB)(context.Background())
		if err != nil || rep == nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(rep.Skipped()) != 1 || rep.Skipped()[0].Name != "a" {
			t.Fatalf("expected node a skipped, got %v", rep.Skipped())
		}
		if len(rep.Successful()) != 1 || rep.Successful()[0].Name != "b" {
			t.Fatalf("expected node b success, got %v", rep.Successful())
		}

		repN, errN := flow.DAGNWithReport(1, nA, nB)(context.Background())
		if errN != nil || repN == nil {
			t.Fatalf("unexpected error: %v", errN)
		}
		if len(repN.Skipped()) != 1 || repN.Skipped()[0].Name != "a" {
			t.Fatalf("expected node a skipped in DAGN, got %v", repN.Skipped())
		}
	})

	t.Run("DAGWithReport and DAGNWithReport concurrent failures and contention", func(t *testing.T) {
		errFast := errors.New("fast error")
		nodes := make([]*flow.DAGNode[context.Context], 25)
		nodes[0] = flow.Node("fail", func(ctx context.Context) error {
			return errFast
		})
		for i := 1; i < 25; i++ {
			nodes[i] = flow.Node(strings.Repeat("n", i), func(ctx context.Context) error {
				time.Sleep(5 * time.Millisecond)
				return nil
			})
		}
		rep, err := flow.DAGWithReport(nodes...)(context.Background())
		if !errors.Is(err, errFast) || rep == nil {
			t.Fatalf("expected fast error, got %v", err)
		}

		repN, errN := flow.DAGNWithReport(2, nodes...)(context.Background())
		if !errors.Is(errN, errFast) || repN == nil {
			t.Fatalf("expected fast error in DAGN, got %v", errN)
		}

		n1Hold := make(chan struct{})
		n2Waiting := make(chan struct{})
		nContend1 := flow.Node("c1", func(ctx context.Context) error {
			<-n1Hold
			return errFast
		})
		nContend2 := flow.Node("c2", func(ctx context.Context) error {
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
		repContend, errContend := flow.DAGNWithReport(1, nContend1, nContend2)(context.Background())
		if !errors.Is(errContend, errFast) || repContend == nil {
			t.Fatalf("expected error on contention, got %v", errContend)
		}
	})

	t.Run("DAGWithReport and DAGNWithReport late cancel after successful steps", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		n1 := flow.Node("n1", func(c context.Context) error {
			cancel()
			return nil
		})
		rep, err := flow.DAGWithReport(n1)(ctx)
		if !errors.Is(err, context.Canceled) || rep == nil {
			t.Fatalf("expected context.Canceled, got err=%v, rep=%v", err, rep)
		}

		ctxN, cancelN := context.WithCancel(context.Background())
		nN1 := flow.Node("n1", func(c context.Context) error { return nil })
		nN2 := flow.Node("n2", func(c context.Context) error {
			cancelN()
			return nil
		}).After("n1")
		repN, errN := flow.DAGNWithReport(1, nN1, nN2)(ctxN)
		if !errors.Is(errN, context.Canceled) || repN == nil {
			t.Fatalf("expected context.Canceled, got err=%v, rep=%v", errN, repN)
		}
	})
}
