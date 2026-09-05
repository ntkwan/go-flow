package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/cucumber/godog"

	"github.com/ntkwan/go-flow"
)

type compositionContext struct {
	seqSteps         []flow.Step[context.Context]
	execLog          []int
	seqErr           error
	primaryExecuted  bool
	chainedExecuted  bool
	fallbackExecuted bool
	chainedStep      flow.Step[context.Context]
	stepWithFallback flow.Step[context.Context]
	lastErr          error
	partitionKeys    []string
	collectedKeys    []string
	branchStep       flow.Step[context.Context]
	ifRan            bool
	elseRan          bool
	pipeStep         flow.Step[*bddPipeContext]
	pipeOutput       string
	streamItems      []int
	streamResults    []int
}

func newCompositionContext() *compositionContext {
	return &compositionContext{}
}

func (c *compositionContext) sequentialStepsThatRecordTheirExecution(count int) error {
	c.seqSteps = nil
	c.execLog = nil
	for i := 1; i <= count; i++ {
		stepNum := i
		c.seqSteps = append(c.seqSteps, func(ctx context.Context) error {
			c.execLog = append(c.execLog, stepNum)
			return nil
		})
	}
	return nil
}

func (c *compositionContext) theSequentialPipelineIsExecuted() error {
	pipeline := flow.Seq(c.seqSteps...)
	c.seqErr = pipeline(context.Background())
	return nil
}

func (c *compositionContext) theStepsExecuteInExactOrder(s1, s2, s3 int) error {
	expected := []int{s1, s2, s3}
	if len(c.execLog) != len(expected) {
		return fmt.Errorf("expected %v, got %v", expected, c.execLog)
	}
	for i, v := range expected {
		if c.execLog[i] != v {
			return fmt.Errorf("at index %d: expected %d, got %d", i, v, c.execLog[i])
		}
	}
	return nil
}

func (c *compositionContext) theSequentialPipelineSucceeds() error {
	if c.seqErr != nil {
		return fmt.Errorf("expected success, got: %w", c.seqErr)
	}
	return nil
}

func (c *compositionContext) sequentialStepsWhereStepFailsWith(count, failIndex int, errMsg string) error {
	c.seqSteps = nil
	c.execLog = nil
	for i := 1; i <= count; i++ {
		stepNum := i
		if stepNum == failIndex {
			c.seqSteps = append(c.seqSteps, func(ctx context.Context) error {
				c.execLog = append(c.execLog, stepNum)
				return errors.New(errMsg)
			})
		} else {
			c.seqSteps = append(c.seqSteps, func(ctx context.Context) error {
				c.execLog = append(c.execLog, stepNum)
				return nil
			})
		}
	}
	return nil
}

func (c *compositionContext) stepExecutes(stepNum int) error {
	for _, num := range c.execLog {
		if num == stepNum {
			return nil
		}
	}
	return fmt.Errorf("step %d was not executed", stepNum)
}

func (c *compositionContext) stepExecutesAndFails(stepNum int) error {
	return c.stepExecutes(stepNum)
}

func (c *compositionContext) stepIsNeverExecuted(stepNum int) error {
	for _, num := range c.execLog {
		if num == stepNum {
			return fmt.Errorf("step %d was unexpectedly executed", stepNum)
		}
	}
	return nil
}

func (c *compositionContext) theSequentialPipelineFailsWith(errMsg string) error {
	if c.seqErr == nil {
		return errors.New("expected error, got nil")
	}
	if !strings.Contains(c.seqErr.Error(), errMsg) {
		return fmt.Errorf("expected error containing %q, got: %w", errMsg, c.seqErr)
	}
	return nil
}

func (c *compositionContext) aPrimaryStepThatSucceeds() error {
	c.chainedStep = flow.Step[context.Context](func(ctx context.Context) error {
		c.primaryExecuted = true
		return nil
	})
	return nil
}

func (c *compositionContext) aChainedStepAttachedWithThen() error {
	next := flow.Step[context.Context](func(ctx context.Context) error {
		c.chainedExecuted = true
		return nil
	})
	c.chainedStep = c.chainedStep.Then(next)
	return nil
}

func (c *compositionContext) theChainedStepIsExecuted() error {
	c.lastErr = c.chainedStep(context.Background())
	return nil
}

func (c *compositionContext) bothStepsExecuteInSequence() error {
	if !c.primaryExecuted || !c.chainedExecuted {
		return fmt.Errorf("expected both steps executed, primary: %v, chained: %v", c.primaryExecuted, c.chainedExecuted)
	}
	return nil
}

func (c *compositionContext) theChainedExecutionSucceeds() error {
	if c.lastErr != nil {
		return fmt.Errorf("expected success, got: %w", c.lastErr)
	}
	return nil
}

func (c *compositionContext) concurrentStepsWritingTheirPartitionKeysToContext(count int) error {
	c.partitionKeys = nil
	for i := 1; i <= count; i++ {
		c.partitionKeys = append(c.partitionKeys, fmt.Sprintf("partition-%d", i))
	}
	return nil
}

func (c *compositionContext) theParallelAggregationStepIsExecuted() error {
	type aggKey struct{}
	type aggState struct {
		mu   sync.Mutex
		keys []string
	}
	st := &aggState{}

	var steps []flow.Step[context.Context]
	for _, k := range c.partitionKeys {
		val := k
		steps = append(steps, func(ctx context.Context) error {
			s := ctx.Value(aggKey{}).(*aggState)
			s.mu.Lock()
			s.keys = append(s.keys, val)
			s.mu.Unlock()
			return nil
		})
	}

	aggStep := flow.Go(steps...)
	ctx := context.WithValue(context.Background(), aggKey{}, st)
	c.lastErr = aggStep(ctx)

	st.mu.Lock()
	c.collectedKeys = append([]string(nil), st.keys...)
	st.mu.Unlock()
	return nil
}

func (c *compositionContext) allPartitionKeysArePresentInContext(count int) error {
	if len(c.collectedKeys) != count {
		return fmt.Errorf("expected %d partition keys, got %d", count, len(c.collectedKeys))
	}
	return nil
}

func (c *compositionContext) theAggregationStepSucceeds() error {
	return c.theChainedExecutionSucceeds()
}

func (c *compositionContext) aPrimaryStepThatFailsWith(errMsg string) error {
	primary := flow.Step[context.Context](func(ctx context.Context) error {
		c.primaryExecuted = true
		return errors.New(errMsg)
	})
	c.stepWithFallback = primary
	return nil
}

func (c *compositionContext) aFallbackStepThatSucceedsOnFailover() error {
	fallback := flow.Step[context.Context](func(ctx context.Context) error {
		c.fallbackExecuted = true
		return nil
	})
	c.stepWithFallback = c.stepWithFallback.Fallback(fallback)
	return nil
}

func (c *compositionContext) aFallbackStepIsAttached() error {
	fallback := flow.Step[context.Context](func(ctx context.Context) error {
		c.fallbackExecuted = true
		return nil
	})
	c.stepWithFallback = c.chainedStep.Fallback(fallback)
	return nil
}

func (c *compositionContext) theStepWithFallbackIsExecuted() error {
	c.lastErr = c.stepWithFallback(context.Background())
	return nil
}

func (c *compositionContext) theFallbackStepExecutes() error {
	if !c.fallbackExecuted {
		return errors.New("expected fallback to execute")
	}
	return nil
}

func (c *compositionContext) theFallbackStepIsNeverExecuted() error {
	if c.fallbackExecuted {
		return errors.New("expected fallback to not execute")
	}
	return nil
}

func (c *compositionContext) theFailoverExecutionSucceeds() error {
	return c.theChainedExecutionSucceeds()
}

func (c *compositionContext) aConditionalBranchWithTrueCondition() error {
	c.ifRan = false
	c.elseRan = false
	c.branchStep = flow.Branch(
		func(ctx context.Context) bool { return true },
		func(ctx context.Context) error {
			c.ifRan = true
			return nil
		},
		func(ctx context.Context) error {
			c.elseRan = true
			return nil
		},
	)
	return nil
}

func (c *compositionContext) aConditionalBranchWithFalseCondition() error {
	c.ifRan = false
	c.elseRan = false
	c.branchStep = flow.Branch(
		func(ctx context.Context) bool { return false },
		func(ctx context.Context) error {
			c.ifRan = true
			return nil
		},
		func(ctx context.Context) error {
			c.elseRan = true
			return nil
		},
	)
	return nil
}

func (c *compositionContext) theConditionalBranchIsExecuted() error {
	c.lastErr = c.branchStep(context.Background())
	return nil
}

func (c *compositionContext) theIfBranchExecutes() error {
	if !c.ifRan {
		return errors.New("expected if-branch to execute")
	}
	return nil
}

func (c *compositionContext) theIfBranchIsNeverExecuted() error {
	if c.ifRan {
		return errors.New("expected if-branch to not execute")
	}
	return nil
}

func (c *compositionContext) theElseBranchExecutes() error {
	if !c.elseRan {
		return errors.New("expected else-branch to execute")
	}
	return nil
}

func (c *compositionContext) theElseBranchIsNeverExecuted() error {
	if c.elseRan {
		return errors.New("expected else-branch to not execute")
	}
	return nil
}

func (c *compositionContext) theBranchExecutionSucceeds() error {
	return c.theChainedExecutionSucceeds()
}

type bddPipeContext struct {
	context.Context
	Input  int
	Output string
}

func (c *compositionContext) aTypedPipeStepThatMultipliesInputBy2() error {
	c.pipeStep = flow.Pipe(
		func(ctx *bddPipeContext, in int) (string, error) {
			return fmt.Sprintf("%d", in*2), nil
		},
		func(ctx *bddPipeContext) int {
			return ctx.Input
		},
		func(ctx *bddPipeContext, out string) {
			ctx.Output = out
		},
	)
	return nil
}

func (c *compositionContext) thePipeStepIsExecutedWithInput(input int) error {
	pCtx := &bddPipeContext{
		Context: context.Background(),
		Input:   input,
	}
	c.lastErr = c.pipeStep(pCtx)
	c.pipeOutput = pCtx.Output
	return nil
}

func (c *compositionContext) theOutputInContextIs(expected string) error {
	if c.pipeOutput != expected {
		return fmt.Errorf("expected output %q, got %q", expected, c.pipeOutput)
	}
	return nil
}

func (c *compositionContext) thePipeExecutionSucceeds() error {
	return c.theChainedExecutionSucceeds()
}

func (c *compositionContext) aStreamOfIntegersFrom1To(count int) error {
	c.streamItems = nil
	for i := 1; i <= count; i++ {
		c.streamItems = append(c.streamItems, i)
	}
	return nil
}

func (c *compositionContext) theStreamIsPipedThroughDoubleTransform() error {
	c.streamResults = nil
	seq := func(yield func(int) bool) {
		for _, v := range c.streamItems {
			if !yield(v) {
				return
			}
		}
	}
	step := flow.PipeSeq(
		seq,
		func(ctx context.Context, item int) (int, error) {
			return item * 2, nil
		},
		func(ctx context.Context, item int) error {
			c.streamResults = append(c.streamResults, item)
			return nil
		},
	)
	c.lastErr = step(context.Background())
	return nil
}

func (c *compositionContext) transformedItemsAreCollectedInOrder(count int) error {
	if len(c.streamResults) != count {
		return fmt.Errorf("expected %d items, got %d", count, len(c.streamResults))
	}
	for i, v := range c.streamResults {
		expected := c.streamItems[i] * 2
		if v != expected {
			return fmt.Errorf("at index %d: expected %d, got %d", i, expected, v)
		}
	}
	return nil
}

func (c *compositionContext) thePipeStreamExecutionSucceeds() error {
	return c.theChainedExecutionSucceeds()
}

func registerCompositionSteps(ctx *godog.ScenarioContext) {
	c := newCompositionContext()

	ctx.Step(`^(\d+) sequential steps that record their execution$`, c.sequentialStepsThatRecordTheirExecution)
	ctx.Step(`^the sequential pipeline is executed$`, c.theSequentialPipelineIsExecuted)
	ctx.Step(`^the steps execute in exact order (\d+), (\d+), (\d+)$`, c.theStepsExecuteInExactOrder)
	ctx.Step(`^the sequential pipeline succeeds$`, c.theSequentialPipelineSucceeds)
	ctx.Step(`^(\d+) sequential steps where step (\d+) fails with "([^"]*)"$`, c.sequentialStepsWhereStepFailsWith)
	ctx.Step(`^step (\d+) executes$`, c.stepExecutes)
	ctx.Step(`^step (\d+) executes and fails$`, c.stepExecutesAndFails)
	ctx.Step(`^step (\d+) is never executed$`, c.stepIsNeverExecuted)
	ctx.Step(`^the sequential pipeline fails with "([^"]*)"$`, c.theSequentialPipelineFailsWith)

	ctx.Step(`^a primary step that succeeds$`, c.aPrimaryStepThatSucceeds)
	ctx.Step(`^a chained step attached with Then$`, c.aChainedStepAttachedWithThen)
	ctx.Step(`^the chained step is executed$`, c.theChainedStepIsExecuted)
	ctx.Step(`^both steps execute in sequence$`, c.bothStepsExecuteInSequence)
	ctx.Step(`^the chained execution succeeds$`, c.theChainedExecutionSucceeds)

	ctx.Step(`^(\d+) concurrent steps writing their partition keys to context$`, c.concurrentStepsWritingTheirPartitionKeysToContext)
	ctx.Step(`^the parallel aggregation step is executed$`, c.theParallelAggregationStepIsExecuted)
	ctx.Step(`^all (\d+) partition keys are present in context$`, c.allPartitionKeysArePresentInContext)
	ctx.Step(`^the aggregation step succeeds$`, c.theAggregationStepSucceeds)

	ctx.Step(`^a primary step that fails with "([^"]*)"$`, c.aPrimaryStepThatFailsWith)
	ctx.Step(`^a fallback step that succeeds on failover$`, c.aFallbackStepThatSucceedsOnFailover)
	ctx.Step(`^a fallback step is attached$`, c.aFallbackStepIsAttached)
	ctx.Step(`^the step with fallback is executed$`, c.theStepWithFallbackIsExecuted)
	ctx.Step(`^the fallback step executes$`, c.theFallbackStepExecutes)
	ctx.Step(`^the fallback step is never executed$`, c.theFallbackStepIsNeverExecuted)
	ctx.Step(`^the failover execution succeeds$`, c.theFailoverExecutionSucceeds)

	ctx.Step(`^a conditional branch with true condition$`, c.aConditionalBranchWithTrueCondition)
	ctx.Step(`^a conditional branch with false condition$`, c.aConditionalBranchWithFalseCondition)
	ctx.Step(`^the conditional branch is executed$`, c.theConditionalBranchIsExecuted)
	ctx.Step(`^the if-branch executes$`, c.theIfBranchExecutes)
	ctx.Step(`^the if-branch is never executed$`, c.theIfBranchIsNeverExecuted)
	ctx.Step(`^the else-branch executes$`, c.theElseBranchExecutes)
	ctx.Step(`^the else-branch is never executed$`, c.theElseBranchIsNeverExecuted)
	ctx.Step(`^the branch execution succeeds$`, c.theBranchExecutionSucceeds)

	ctx.Step(`^a typed pipe step that multiplies input by 2$`, c.aTypedPipeStepThatMultipliesInputBy2)
	ctx.Step(`^the pipe step is executed with input (\d+)$`, c.thePipeStepIsExecutedWithInput)
	ctx.Step(`^the output in context is "([^"]*)"$`, c.theOutputInContextIs)
	ctx.Step(`^the pipe execution succeeds$`, c.thePipeExecutionSucceeds)

	ctx.Step(`^a stream of (\d+) integers from 1 to (\d+)$`, c.aStreamOfIntegersFrom1To)
	ctx.Step(`^the stream is piped through double transform$`, c.theStreamIsPipedThroughDoubleTransform)
	ctx.Step(`^(\d+) transformed items are collected in order$`, c.transformedItemsAreCollectedInOrder)
	ctx.Step(`^the pipe stream execution succeeds$`, c.thePipeStreamExecutionSucceeds)
}
