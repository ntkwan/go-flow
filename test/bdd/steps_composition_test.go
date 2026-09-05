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
}
