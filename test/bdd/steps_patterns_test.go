package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cucumber/godog"

	"github.com/ntkwan/go-flow"
)

type patternsContext struct {
	nodes           map[string]*flow.DAGNode[context.Context]
	startTimes      map[string]time.Time
	endTimes        map[string]time.Time
	execOrder       []string
	mu              sync.Mutex
	dagErr          error
	parallelSteps   []flow.Step[context.Context]
	parallelErr     error
	completedCount  atomic.Int32
	activeCount     atomic.Int32
	maxActive       atomic.Int32
	scatterResults  []string
	gatheredResults []string
	mermaidOutput   string
}

func newPatternsContext() *patternsContext {
	return &patternsContext{
		nodes:      make(map[string]*flow.DAGNode[context.Context]),
		startTimes: make(map[string]time.Time),
		endTimes:   make(map[string]time.Time),
	}
}

func (c *patternsContext) aDAGWithRootNode(name string) error {
	c.nodes[name] = flow.Node(name, func(ctx context.Context) error {
		c.mu.Lock()
		c.startTimes[name] = time.Now()
		c.execOrder = append(c.execOrder, name)
		c.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		c.mu.Lock()
		c.endTimes[name] = time.Now()
		c.mu.Unlock()
		return nil
	})
	return nil
}

func (c *patternsContext) aNodeThatDependsOn(name, dep string) error {
	c.nodes[name] = flow.Node(name, func(ctx context.Context) error {
		c.mu.Lock()
		c.startTimes[name] = time.Now()
		c.execOrder = append(c.execOrder, name)
		c.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		c.mu.Lock()
		c.endTimes[name] = time.Now()
		c.mu.Unlock()
		return nil
	}).After(dep)
	return nil
}

func (c *patternsContext) aDAGWithRootNodeThatReturnsAnError(name string) error {
	c.nodes[name] = flow.Node(name, func(ctx context.Context) error {
		c.mu.Lock()
		c.execOrder = append(c.execOrder, name)
		c.mu.Unlock()
		return errors.New("node " + name + " failure")
	})
	return nil
}

func (c *patternsContext) aDAGWithIndependentNodesAnd(name1, name2 string) error {
	c.nodes[name1] = flow.Node(name1, func(ctx context.Context) error {
		c.mu.Lock()
		c.startTimes[name1] = time.Now()
		c.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		c.mu.Lock()
		c.endTimes[name1] = time.Now()
		c.mu.Unlock()
		return nil
	})
	c.nodes[name2] = flow.Node(name2, func(ctx context.Context) error {
		c.mu.Lock()
		c.startTimes[name2] = time.Now()
		c.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		c.mu.Lock()
		c.endTimes[name2] = time.Now()
		c.mu.Unlock()
		return nil
	})
	return nil
}

func (c *patternsContext) theDAGIsExecuted() error {
	var nodeList []*flow.DAGNode[context.Context]
	for _, n := range c.nodes {
		nodeList = append(nodeList, n)
	}
	dag := flow.DAG(nodeList...)
	c.dagErr = dag(context.Background())
	return nil
}

func (c *patternsContext) nodeCompletesBeforeStarts(first, second string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	endFirst, ok1 := c.endTimes[first]
	startSecond, ok2 := c.startTimes[second]
	if !ok1 || !ok2 {
		return fmt.Errorf("missing timestamp for %s or %s", first, second)
	}
	if !endFirst.Before(startSecond) && !endFirst.Equal(startSecond) {
		return fmt.Errorf("node %s ended at %v, but %s started at %v", first, endFirst, second, startSecond)
	}
	return nil
}

func (c *patternsContext) theDAGExecutionSucceeds() error {
	if c.dagErr != nil {
		return fmt.Errorf("expected dag success, got: %w", c.dagErr)
	}
	return nil
}

func (c *patternsContext) nodeIsNeverExecuted(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, exec := range c.execOrder {
		if exec == name {
			return fmt.Errorf("node %s was executed", name)
		}
	}
	return nil
}

func (c *patternsContext) theDAGExecutionFailsWithNodeError(name string) error {
	if c.dagErr == nil {
		return errors.New("expected dag error, got nil")
	}
	if !strings.Contains(c.dagErr.Error(), "node "+name+" failure") {
		return fmt.Errorf("expected error from %s, got: %w", name, c.dagErr)
	}
	return nil
}

func (c *patternsContext) nodesAndExecuteConcurrently(name1, name2 string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	start1 := c.startTimes[name1]
	end1 := c.endTimes[name1]
	start2 := c.startTimes[name2]
	end2 := c.endTimes[name2]
	if end1.Before(start2) || end2.Before(start1) {
		return fmt.Errorf("nodes %s and %s did not overlap in execution", name1, name2)
	}
	return nil
}

func (c *patternsContext) parallelStepsThatSucceed(count int) error {
	c.parallelSteps = nil
	for i := 0; i < count; i++ {
		c.parallelSteps = append(c.parallelSteps, func(ctx context.Context) error {
			c.completedCount.Add(1)
			return nil
		})
	}
	return nil
}

func (c *patternsContext) parallelStepsThatSucceedAndThatFailsWith(successCount, failCount int, errMsg string) error {
	c.parallelSteps = nil
	for i := 0; i < successCount; i++ {
		c.parallelSteps = append(c.parallelSteps, func(ctx context.Context) error {
			c.completedCount.Add(1)
			return nil
		})
	}
	for i := 0; i < failCount; i++ {
		c.parallelSteps = append(c.parallelSteps, func(ctx context.Context) error {
			return errors.New(errMsg)
		})
	}
	return nil
}

func (c *patternsContext) parallelStepsThatFailWithErrorsAnd(count int, err1, err2 string) error {
	c.parallelSteps = []flow.Step[context.Context]{
		func(ctx context.Context) error { return errors.New(err1) },
		func(ctx context.Context) error { return errors.New(err2) },
	}
	return nil
}

func (c *patternsContext) parallelStepsWithAConcurrencyLimitOf(total, limit int) error {
	c.parallelSteps = nil
	for i := 0; i < total; i++ {
		c.parallelSteps = append(c.parallelSteps, func(ctx context.Context) error {
			cur := c.activeCount.Add(1)
			for {
				max := c.maxActive.Load()
				if cur <= max || c.maxActive.CompareAndSwap(max, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			c.activeCount.Add(-1)
			c.completedCount.Add(1)
			return nil
		})
	}
	return nil
}

func (c *patternsContext) theParallelFlowIsExecuted() error {
	f := flow.Go(c.parallelSteps...)
	c.parallelErr = f(context.Background())
	return nil
}

func (c *patternsContext) theBoundedParallelFlowIsExecuted() error {
	f := flow.GoN(2, c.parallelSteps...)
	c.parallelErr = f(context.Background())
	return nil
}

func (c *patternsContext) allStepsComplete(expected int) error {
	if int(c.completedCount.Load()) != expected {
		return fmt.Errorf("expected %d completed steps, got %d", expected, c.completedCount.Load())
	}
	return nil
}

func (c *patternsContext) theParallelFlowSucceeds() error {
	if c.parallelErr != nil {
		return fmt.Errorf("expected success, got: %w", c.parallelErr)
	}
	return nil
}

func (c *patternsContext) theBoundedParallelFlowSucceeds() error {
	if c.parallelErr != nil {
		return fmt.Errorf("expected success, got: %w", c.parallelErr)
	}
	return nil
}

func (c *patternsContext) theParallelFlowFailsWithErrorContaining(errMsg string) error {
	if c.parallelErr == nil {
		return errors.New("expected error, got nil")
	}
	if !strings.Contains(c.parallelErr.Error(), errMsg) {
		return fmt.Errorf("expected error containing %q, got: %w", errMsg, c.parallelErr)
	}
	return nil
}

func (c *patternsContext) theParallelFlowFailsWithJoinedErrorContainingAnd(err1, err2 string) error {
	if c.parallelErr == nil {
		return errors.New("expected error, got nil")
	}
	if !strings.Contains(c.parallelErr.Error(), err1) || !strings.Contains(c.parallelErr.Error(), err2) {
		return fmt.Errorf("expected error containing %q and %q, got: %w", err1, err2, c.parallelErr)
	}
	return nil
}

func (c *patternsContext) atMostStepsRunConcurrently(limit int) error {
	if int(c.maxActive.Load()) > limit {
		return fmt.Errorf("expected max %d active, observed %d", limit, c.maxActive.Load())
	}
	return nil
}

func (c *patternsContext) scatterStepsProducingResults(count int, r1, r2, r3 string) error {
	c.scatterResults = []string{r1, r2, r3}
	return nil
}

func (c *patternsContext) aGatherStepThatCollectsAllResultsIntoAnAggregate() error {
	return nil
}

func (c *patternsContext) theScattergatherWorkflowIsExecuted() error {
	type resultKey struct{}
	type state struct {
		mu   sync.Mutex
		data []string
	}
	st := &state{}

	var steps []flow.Step[context.Context]
	for _, item := range c.scatterResults {
		val := item
		steps = append(steps, func(ctx context.Context) error {
			s := ctx.Value(resultKey{}).(*state)
			s.mu.Lock()
			s.data = append(s.data, val)
			s.mu.Unlock()
			return nil
		})
	}

	scatter := flow.Go(steps...)
	gather := func(ctx context.Context) error {
		s := ctx.Value(resultKey{}).(*state)
		s.mu.Lock()
		c.gatheredResults = append([]string(nil), s.data...)
		s.mu.Unlock()
		return nil
	}

	pipeline := flow.Seq(scatter, gather)
	ctx := context.WithValue(context.Background(), resultKey{}, st)
	return pipeline(ctx)
}

func (c *patternsContext) theAggregateContains(r1, r2, r3 string) error {
	expected := map[string]bool{r1: true, r2: true, r3: true}
	for _, res := range c.gatheredResults {
		delete(expected, res)
	}
	if len(expected) > 0 {
		return fmt.Errorf("missing results: %v, got %v", expected, c.gatheredResults)
	}
	return nil
}

func (c *patternsContext) theScattergatherWorkflowSucceeds() error {
	if len(c.gatheredResults) != len(c.scatterResults) {
		return fmt.Errorf("expected %d gathered results, got %d", len(c.scatterResults), len(c.gatheredResults))
	}
	return nil
}

func (c *patternsContext) aDAGWithDependencyBetweenAnd(from, to string) error {
	c.nodes[from] = flow.Node(from, func(ctx context.Context) error { return nil })
	c.nodes[to] = flow.Node(to, func(ctx context.Context) error { return nil }).After(from)
	return nil
}

func (c *patternsContext) aDAGWithAnIsolatedNode(name string) error {
	c.nodes[name] = flow.Node(name, func(ctx context.Context) error { return nil })
	return nil
}

func (c *patternsContext) theDAGIsExportedToMermaidFormat() error {
	var nodeList []*flow.DAGNode[context.Context]
	for _, n := range c.nodes {
		nodeList = append(nodeList, n)
	}
	out, err := flow.DAGToMermaid(nodeList...)
	if err != nil {
		return err
	}
	c.mermaidOutput = out
	return nil
}

func (c *patternsContext) theMermaidOutputContainsEdgeFromTo(from, to string) error {
	escapedFrom := strings.ReplaceAll(from, "-", "_")
	escapedTo := strings.ReplaceAll(to, "-", "_")
	expectedEdge := fmt.Sprintf("%s[\"%s\"] --> %s[\"%s\"]", escapedFrom, from, escapedTo, to)
	if !strings.Contains(c.mermaidOutput, expectedEdge) {
		return fmt.Errorf("expected mermaid output to contain %q, got:\n%s", expectedEdge, c.mermaidOutput)
	}
	return nil
}

func (c *patternsContext) theMermaidOutputStartsWith(prefix string) error {
	if !strings.HasPrefix(c.mermaidOutput, prefix) {
		return fmt.Errorf("expected mermaid output to start with %q, got:\n%s", prefix, c.mermaidOutput)
	}
	return nil
}

func (c *patternsContext) theMermaidOutputContainsNode(name string) error {
	escaped := strings.ReplaceAll(name, "-", "_")
	expectedNode := fmt.Sprintf("%s[\"%s\"]", escaped, name)
	if !strings.Contains(c.mermaidOutput, expectedNode) {
		return fmt.Errorf("expected mermaid output to contain %q, got:\n%s", expectedNode, c.mermaidOutput)
	}
	return nil
}

func registerPatternsSteps(ctx *godog.ScenarioContext) {
	c := newPatternsContext()
	ctx.Step(`^a DAG with root node "([^"]*)"$`, c.aDAGWithRootNode)
	ctx.Step(`^a node "([^"]*)" that depends on "([^"]*)"$`, c.aNodeThatDependsOn)
	ctx.Step(`^a DAG with root node "([^"]*)" that returns an error$`, c.aDAGWithRootNodeThatReturnsAnError)
	ctx.Step(`^a DAG with independent nodes "([^"]*)" and "([^"]*)"$`, c.aDAGWithIndependentNodesAnd)
	ctx.Step(`^the DAG is executed$`, c.theDAGIsExecuted)
	ctx.Step(`^node "([^"]*)" completes before "([^"]*)" starts$`, c.nodeCompletesBeforeStarts)
	ctx.Step(`^the DAG execution succeeds$`, c.theDAGExecutionSucceeds)
	ctx.Step(`^node "([^"]*)" is never executed$`, c.nodeIsNeverExecuted)
	ctx.Step(`^the DAG execution fails with node "([^"]*)" error$`, c.theDAGExecutionFailsWithNodeError)
	ctx.Step(`^nodes "([^"]*)" and "([^"]*)" execute concurrently$`, c.nodesAndExecuteConcurrently)

	ctx.Step(`^(\d+) parallel steps that succeed$`, c.parallelStepsThatSucceed)
	ctx.Step(`^(\d+) parallel steps that succeed and (\d+) that fails with "([^"]*)"$`, c.parallelStepsThatSucceedAndThatFailsWith)
	ctx.Step(`^(\d+) parallel steps that fail with errors "([^"]*)" and "([^"]*)"$`, c.parallelStepsThatFailWithErrorsAnd)
	ctx.Step(`^(\d+) parallel steps with a concurrency limit of (\d+)$`, c.parallelStepsWithAConcurrencyLimitOf)
	ctx.Step(`^the parallel flow is executed$`, c.theParallelFlowIsExecuted)
	ctx.Step(`^the bounded parallel flow is executed$`, c.theBoundedParallelFlowIsExecuted)
	ctx.Step(`^all (\d+) steps complete$`, c.allStepsComplete)
	ctx.Step(`^the parallel flow succeeds$`, c.theParallelFlowSucceeds)
	ctx.Step(`^the bounded parallel flow succeeds$`, c.theBoundedParallelFlowSucceeds)
	ctx.Step(`^the parallel flow fails with error containing "([^"]*)"$`, c.theParallelFlowFailsWithErrorContaining)
	ctx.Step(`^the parallel flow fails with joined error containing "([^"]*)" and "([^"]*)"$`, c.theParallelFlowFailsWithJoinedErrorContainingAnd)
	ctx.Step(`^at most (\d+) steps run concurrently$`, c.atMostStepsRunConcurrently)

	ctx.Step(`^(\d+) scatter steps producing results "([^"]*)", "([^"]*)", "([^"]*)"$`, c.scatterStepsProducingResults)
	ctx.Step(`^a gather step that collects all results into an aggregate$`, c.aGatherStepThatCollectsAllResultsIntoAnAggregate)
	ctx.Step(`^the scatter-gather workflow is executed$`, c.theScattergatherWorkflowIsExecuted)
	ctx.Step(`^the aggregate contains "([^"]*)", "([^"]*)", "([^"]*)"$`, c.theAggregateContains)
	ctx.Step(`^the scatter-gather workflow succeeds$`, c.theScattergatherWorkflowSucceeds)

	ctx.Step(`^a DAG with dependency between "([^"]*)" and "([^"]*)"$`, c.aDAGWithDependencyBetweenAnd)
	ctx.Step(`^a DAG with an isolated node "([^"]*)"$`, c.aDAGWithAnIsolatedNode)
	ctx.Step(`^the DAG is exported to Mermaid format$`, c.theDAGIsExportedToMermaidFormat)
	ctx.Step(`^the Mermaid output contains edge from "([^"]*)" to "([^"]*)"$`, c.theMermaidOutputContainsEdgeFromTo)
	ctx.Step(`^the Mermaid output starts with "([^"]*)"$`, c.theMermaidOutputStartsWith)
	ctx.Step(`^the Mermaid output contains node "([^"]*)"$`, c.theMermaidOutputContainsNode)
}
