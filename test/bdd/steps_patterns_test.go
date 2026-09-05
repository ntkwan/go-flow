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
	report          *flow.DAGReport
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

func (c *patternsContext) aNodeThatDependsOnWithFalseCondition(name, dep string) error {
	c.nodes[name] = flow.Node(name, func(ctx context.Context) error {
		c.mu.Lock()
		c.execOrder = append(c.execOrder, name)
		c.mu.Unlock()
		return nil
	}).After(dep).When(func(ctx context.Context) bool { return false })
	return nil
}

func (c *patternsContext) aDAGWithAFailingNode(name string) error {
	c.nodes[name] = flow.Node(name, func(ctx context.Context) error {
		c.mu.Lock()
		c.execOrder = append(c.execOrder, name)
		c.mu.Unlock()
		return errors.New("node " + name + " failed")
	})
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

func (c *patternsContext) theDAGIsExecutedWithAReport() error {
	var nodeList []*flow.DAGNode[context.Context]
	for _, n := range c.nodes {
		nodeList = append(nodeList, n)
	}
	exec := flow.DAGWithReport(nodeList...)
	c.report, c.dagErr = exec(context.Background())
	return nil
}

func (c *patternsContext) nodeStatusIs(name, expectedStatus string) error {
	if c.report == nil {
		return errors.New("expected report to be non-nil")
	}
	nodeRep := c.report.Node(name)
	if nodeRep == nil {
		return fmt.Errorf("node %q not found in report", name)
	}
	if string(nodeRep.Status) != expectedStatus {
		return fmt.Errorf("expected node %q status %s, got %s", name, expectedStatus, nodeRep.Status)
	}
	return nil
}

func (c *patternsContext) nodeReportsAnError(name string) error {
	if c.report == nil {
		return errors.New("expected report to be non-nil")
	}
	nodeRep := c.report.Node(name)
	if nodeRep == nil || nodeRep.Err == nil {
		return fmt.Errorf("expected node %q to have error in report", name)
	}
	return nil
}

func (c *patternsContext) nodeIsNotExecuted(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, executed := range c.execOrder {
		if executed == name {
			return fmt.Errorf("expected node %q not to be executed", name)
		}
	}
	return nil
}

func (c *patternsContext) theOverallDAGExecutionSucceeds() error {
	if c.dagErr != nil {
		return fmt.Errorf("expected DAG to succeed, got: %v", c.dagErr)
	}
	return nil
}

func (c *patternsContext) theReportRecordsAPositiveExecutionDuration() error {
	if c.report == nil {
		return errors.New("expected report to be non-nil")
	}
	if c.report.Duration < 0 {
		return fmt.Errorf("expected positive duration, got %v", c.report.Duration)
	}
	return nil
}

func (c *patternsContext) nodeCompletesBeforeStarts(nodeA, nodeB string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	endA, okA := c.endTimes[nodeA]
	startB, okB := c.startTimes[nodeB]
	if !okA || !okB {
		return fmt.Errorf("missing timestamps: %s=%v, %s=%v", nodeA, okA, nodeB, okB)
	}
	if endA.After(startB) {
		return fmt.Errorf("node %s ended at %v after %s started at %v", nodeA, endA, nodeB, startB)
	}
	return nil
}

func (c *patternsContext) theDAGExecutionSucceeds() error {
	if c.dagErr != nil {
		return fmt.Errorf("expected DAG to succeed, got: %v", c.dagErr)
	}
	return nil
}

func (c *patternsContext) nodeIsNeverExecuted(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.execOrder {
		if n == name {
			return fmt.Errorf("node %s was executed", name)
		}
	}
	return nil
}

func (c *patternsContext) theDAGExecutionFailsWithNodeError(name string) error {
	if c.dagErr == nil {
		return errors.New("expected DAG to fail, but it succeeded")
	}
	expected := "node " + name + " failure"
	if !strings.Contains(c.dagErr.Error(), expected) {
		return fmt.Errorf("expected error containing %q, got: %v", expected, c.dagErr)
	}
	return nil
}

func (c *patternsContext) nodesAndExecuteConcurrently(name1, name2 string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	start1, ok1 := c.startTimes[name1]
	start2, ok2 := c.startTimes[name2]
	if !ok1 || !ok2 {
		return fmt.Errorf("missing timestamps for concurrent check")
	}
	diff := start1.Sub(start2)
	if diff < 0 {
		diff = -diff
	}
	if diff > 15*time.Millisecond {
		return fmt.Errorf("nodes %s and %s did not start concurrently, diff: %v", name1, name2, diff)
	}
	return nil
}

func (c *patternsContext) parallelStepsThatSucceed(count int) error {
	c.parallelSteps = make([]flow.Step[context.Context], count)
	for i := 0; i < count; i++ {
		c.parallelSteps[i] = func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			c.completedCount.Add(1)
			return nil
		}
	}
	return nil
}

func (c *patternsContext) parallelStepsThatSucceedAndThatFailsWith(successCount, failCount int, errMsg string) error {
	c.parallelSteps = make([]flow.Step[context.Context], successCount+failCount)
	for i := 0; i < successCount; i++ {
		c.parallelSteps[i] = func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			c.completedCount.Add(1)
			return nil
		}
	}
	for i := 0; i < failCount; i++ {
		c.parallelSteps[successCount+i] = func(ctx context.Context) error {
			return errors.New(errMsg)
		}
	}
	return nil
}

func (c *patternsContext) parallelStepsThatFailWithErrorsAnd(count int, err1, err2 string) error {
	errMessages := []string{err1, err2}
	c.parallelSteps = make([]flow.Step[context.Context], count)
	for i := 0; i < count; i++ {
		msg := errMessages[i%len(errMessages)]
		c.parallelSteps[i] = func(ctx context.Context) error {
			return errors.New(msg)
		}
	}
	return nil
}

func (c *patternsContext) parallelStepsWithAConcurrencyLimitOf(count, limit int) error {
	c.parallelSteps = make([]flow.Step[context.Context], count)
	for i := 0; i < count; i++ {
		c.parallelSteps[i] = func(ctx context.Context) error {
			cur := c.activeCount.Add(1)
			for {
				old := c.maxActive.Load()
				if cur <= old || c.maxActive.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			c.activeCount.Add(-1)
			c.completedCount.Add(1)
			return nil
		}
	}
	return nil
}

func (c *patternsContext) theParallelFlowIsExecuted() error {
	parallelFlow := flow.Go(c.parallelSteps...)
	c.parallelErr = parallelFlow(context.Background())
	return nil
}

func (c *patternsContext) theBoundedParallelFlowIsExecuted() error {
	boundedFlow := flow.GoN(2, c.parallelSteps...)
	c.parallelErr = boundedFlow(context.Background())
	return nil
}

func (c *patternsContext) allStepsComplete(count int) error {
	if int(c.completedCount.Load()) != count {
		return fmt.Errorf("expected %d completed steps, got %d", count, c.completedCount.Load())
	}
	return nil
}

func (c *patternsContext) theParallelFlowSucceeds() error {
	if c.parallelErr != nil {
		return fmt.Errorf("expected parallel flow to succeed, got: %v", c.parallelErr)
	}
	return nil
}

func (c *patternsContext) theBoundedParallelFlowSucceeds() error {
	if c.parallelErr != nil {
		return fmt.Errorf("expected bounded parallel flow to succeed, got: %v", c.parallelErr)
	}
	return nil
}

func (c *patternsContext) theParallelFlowFailsWithErrorContaining(errMsg string) error {
	if c.parallelErr == nil {
		return errors.New("expected parallel flow to fail, got nil")
	}
	if !strings.Contains(c.parallelErr.Error(), errMsg) {
		return fmt.Errorf("expected error containing %q, got: %v", errMsg, c.parallelErr)
	}
	return nil
}

func (c *patternsContext) theParallelFlowFailsWithJoinedErrorContainingAnd(err1, err2 string) error {
	if c.parallelErr == nil {
		return errors.New("expected joined error, got nil")
	}
	if !strings.Contains(c.parallelErr.Error(), err1) || !strings.Contains(c.parallelErr.Error(), err2) {
		return fmt.Errorf("expected error containing both %q and %q, got: %v", err1, err2, c.parallelErr)
	}
	return nil
}

func (c *patternsContext) atMostStepsRunConcurrently(limit int) error {
	if int(c.maxActive.Load()) > limit {
		return fmt.Errorf("expected max %d active goroutines, got %d", limit, c.maxActive.Load())
	}
	return nil
}

func (c *patternsContext) scatterStepsProducingResults(count int, r1, r2, r3 string) error {
	results := []string{r1, r2, r3}
	c.scatterResults = make([]string, count)
	c.parallelSteps = make([]flow.Step[context.Context], count)
	for i := 0; i < count; i++ {
		idx := i
		c.parallelSteps[i] = func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			c.mu.Lock()
			c.scatterResults[idx] = results[idx]
			c.mu.Unlock()
			return nil
		}
	}
	return nil
}

func (c *patternsContext) aGatherStepThatCollectsAllResultsIntoAnAggregate() error {
	return nil
}

func (c *patternsContext) theScattergatherWorkflowIsExecuted() error {
	scatter := flow.Go(c.parallelSteps...)
	gather := func(ctx context.Context) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.gatheredResults = append([]string{}, c.scatterResults...)
		return nil
	}
	workflow := flow.Seq(scatter, gather)
	c.parallelErr = workflow(context.Background())
	return nil
}

func (c *patternsContext) theAggregateContains(r1, r2, r3 string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	expected := []string{r1, r2, r3}
	if len(c.gatheredResults) != len(expected) {
		return fmt.Errorf("expected %d results, got %d", len(expected), len(c.gatheredResults))
	}
	for i, r := range expected {
		if c.gatheredResults[i] != r {
			return fmt.Errorf("expected result %d to be %q, got %q", i, r, c.gatheredResults[i])
		}
	}
	return nil
}

func (c *patternsContext) theScattergatherWorkflowSucceeds() error {
	if c.parallelErr != nil {
		return fmt.Errorf("expected scatter-gather workflow to succeed, got: %v", c.parallelErr)
	}
	return nil
}

func (c *patternsContext) aDAGWithDependencyBetweenAnd(parent, child string) error {
	pStep := func(ctx context.Context) error { return nil }
	cStep := func(ctx context.Context) error { return nil }
	c.nodes[parent] = flow.Node(parent, pStep)
	c.nodes[child] = flow.Node(child, cStep).After(parent)
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
	var err error
	c.mermaidOutput, err = flow.DAGToMermaid(nodeList...)
	return err
}

func (c *patternsContext) theMermaidOutputContainsEdgeFromTo(from, to string) error {
	escapedFrom := strings.ReplaceAll(from, "-", "_")
	escapedTo := strings.ReplaceAll(to, "-", "_")
	expectedEdge := fmt.Sprintf("%s[\"%s\"] --> %s[\"%s\"]", escapedFrom, from, escapedTo, to)
	if !strings.Contains(c.mermaidOutput, expectedEdge) {
		return fmt.Errorf("expected mermaid output to contain edge %q, got:\n%s", expectedEdge, c.mermaidOutput)
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
	ctx.Step(`^a node "([^"]*)" that depends on "([^"]*)" with a false condition$`, c.aNodeThatDependsOnWithFalseCondition)
	ctx.Step(`^a DAG with a failing node "([^"]*)"$`, c.aDAGWithAFailingNode)
	ctx.Step(`^the DAG is executed with a report$`, c.theDAGIsExecutedWithAReport)
	ctx.Step(`^node "([^"]*)" status is "([^"]*)"$`, c.nodeStatusIs)
	ctx.Step(`^node "([^"]*)" reports an error$`, c.nodeReportsAnError)
	ctx.Step(`^node "([^"]*)" is not executed$`, c.nodeIsNotExecuted)
	ctx.Step(`^the overall DAG execution succeeds$`, c.theOverallDAGExecutionSucceeds)
	ctx.Step(`^the report records a positive execution duration$`, c.theReportRecordsAPositiveExecutionDuration)
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
