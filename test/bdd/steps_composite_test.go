package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cucumber/godog"
	"github.com/ntkwan/go-flow"
)

type compositeContext struct {
	nodeAttempts     atomic.Int32
	maxRetries       int
	nodeErrMsg       string
	nodeTimeout      time.Duration
	nodeDuration     time.Duration
	dagResultErr     error
	raceFastestWon   bool
	raceLosersCancel atomic.Int32
	raceSteps        []flow.Step[context.Context]
	raceResultErr    error
	raceFallbackDone bool
	raceWithFallback flow.Step[context.Context]
}

func newCompositeContext() *compositeContext {
	return &compositeContext{}
}

func (c *compositeContext) aDAGNodeConfiguredToRetryUpToTimes(retries int) error {
	c.maxRetries = retries
	c.nodeAttempts.Store(0)
	return nil
}

func (c *compositeContext) theNodeFailsOnAttempt1AndSucceedsOnAttempt2() error {
	return nil
}

func (c *compositeContext) theResilientDAGIsExecuted() error {
	var step flow.Step[context.Context]
	if c.nodeTimeout > 0 {
		step = flow.Step[context.Context](func(ctx context.Context) error {
			select {
			case <-time.After(c.nodeDuration):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}).Timeout(c.nodeTimeout)
	} else if c.nodeErrMsg != "" {
		step = flow.Step[context.Context](func(ctx context.Context) error {
			c.nodeAttempts.Add(1)
			return errors.New(c.nodeErrMsg)
		}).Retry(c.maxRetries, 5*time.Millisecond)
	} else {
		step = flow.Step[context.Context](func(ctx context.Context) error {
			attempt := c.nodeAttempts.Add(1)
			if attempt == 1 {
				return errors.New("transient error")
			}
			return nil
		}).Retry(c.maxRetries, 5*time.Millisecond)
	}

	node := flow.Node("resilient-node", step)
	dag := flow.DAG(node)
	c.dagResultErr = dag(context.Background())
	return nil
}

func (c *compositeContext) theNodeExecutesTimes(expected int) error {
	if int(c.nodeAttempts.Load()) != expected {
		return fmt.Errorf("expected %d attempts, got %d", expected, c.nodeAttempts.Load())
	}
	return nil
}

func (c *compositeContext) theResilientDAGExecutionSucceeds() error {
	if c.dagResultErr != nil {
		return fmt.Errorf("expected dag success, got: %w", c.dagResultErr)
	}
	return nil
}

func (c *compositeContext) theNodeFailsOnEveryAttemptWith(errMsg string) error {
	c.nodeErrMsg = errMsg
	return nil
}

func (c *compositeContext) theNodeExecutesTimesTotal(expected int) error {
	return c.theNodeExecutesTimes(expected)
}

func (c *compositeContext) theResilientDAGExecutionFailsWith(errMsg string) error {
	if c.dagResultErr == nil {
		return errors.New("expected dag error, got nil")
	}
	if !strings.Contains(c.dagResultErr.Error(), errMsg) {
		return fmt.Errorf("expected error containing %q, got: %w", errMsg, c.dagResultErr)
	}
	return nil
}

func (c *compositeContext) aDAGNodeConfiguredWithAMillisecondTimeout(timeoutMs int) error {
	c.nodeTimeout = time.Duration(timeoutMs) * time.Millisecond
	return nil
}

func (c *compositeContext) theNodeStepTakesMillisecondsToComplete(durationMs int) error {
	c.nodeDuration = time.Duration(durationMs) * time.Millisecond
	return nil
}

func (c *compositeContext) theResilientDAGExecutionFailsWithDeadlineExceededError() error {
	if c.dagResultErr == nil {
		return errors.New("expected deadline error, got nil")
	}
	if !errors.Is(c.dagResultErr, context.DeadlineExceeded) && !strings.Contains(c.dagResultErr.Error(), "context deadline exceeded") {
		return fmt.Errorf("expected deadline exceeded, got: %w", c.dagResultErr)
	}
	return nil
}

func (c *compositeContext) threeRacingStepsWithDurations() error {
	c.raceSteps = []flow.Step[context.Context]{
		func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			c.raceFastestWon = true
			return nil
		},
		func(ctx context.Context) error {
			select {
			case <-time.After(100 * time.Millisecond):
				return nil
			case <-ctx.Done():
				c.raceLosersCancel.Add(1)
				return ctx.Err()
			}
		},
		func(ctx context.Context) error {
			select {
			case <-time.After(200 * time.Millisecond):
				return nil
			case <-ctx.Done():
				c.raceLosersCancel.Add(1)
				return ctx.Err()
			}
		},
	}
	return nil
}

func (c *compositeContext) theRacingStepsAreExecuted() error {
	racer := flow.Race(c.raceSteps...)
	c.raceResultErr = racer(context.Background())
	time.Sleep(20 * time.Millisecond)
	return nil
}

func (c *compositeContext) theFastestStepWins() error {
	if !c.raceFastestWon {
		return errors.New("expected fastest step to win")
	}
	return nil
}

func (c *compositeContext) losingBranchesAreCancelled() error {
	if c.raceLosersCancel.Load() == 0 {
		return errors.New("expected losing branches to receive cancellation")
	}
	return nil
}

func (c *compositeContext) theRaceExecutionSucceeds() error {
	if c.raceResultErr != nil {
		return fmt.Errorf("expected race success, got: %w", c.raceResultErr)
	}
	return nil
}

func (c *compositeContext) racingStepsThatAllFailWithError(count int, errMsg string) error {
	c.raceSteps = nil
	for i := 0; i < count; i++ {
		c.raceSteps = append(c.raceSteps, func(ctx context.Context) error {
			return errors.New(errMsg)
		})
	}
	return nil
}

func (c *compositeContext) theRaceExecutionFails() error {
	if c.raceResultErr == nil {
		return errors.New("expected race error, got nil")
	}
	return nil
}

func (c *compositeContext) aRaceFallbackStepThatSucceeds() error {
	fallback := flow.Step[context.Context](func(ctx context.Context) error {
		c.raceFallbackDone = true
		return nil
	})
	c.raceWithFallback = flow.Race(c.raceSteps...).Fallback(fallback)
	return nil
}

func (c *compositeContext) theRacingStepsWithFallbackAreExecuted() error {
	c.raceResultErr = c.raceWithFallback(context.Background())
	return nil
}

func (c *compositeContext) theRaceFallbackStepExecutes() error {
	if !c.raceFallbackDone {
		return errors.New("expected race fallback to execute")
	}
	return nil
}

func (c *compositeContext) theRaceExecutionWithFallbackSucceeds() error {
	if c.raceResultErr != nil {
		return fmt.Errorf("expected race fallback success, got: %w", c.raceResultErr)
	}
	return nil
}

func registerCompositeSteps(ctx *godog.ScenarioContext) {
	c := newCompositeContext()

	ctx.Step(`^a DAG node configured to retry up to (\d+) times$`, c.aDAGNodeConfiguredToRetryUpToTimes)
	ctx.Step(`^the node fails on attempt 1 and succeeds on attempt 2$`, c.theNodeFailsOnAttempt1AndSucceedsOnAttempt2)
	ctx.Step(`^when the resilient DAG is executed$`, c.theResilientDAGIsExecuted)
	ctx.Step(`^the resilient DAG is executed$`, c.theResilientDAGIsExecuted)
	ctx.Step(`^the node executes (\d+) times$`, c.theNodeExecutesTimes)
	ctx.Step(`^the resilient DAG execution succeeds$`, c.theResilientDAGExecutionSucceeds)

	ctx.Step(`^the node fails on every attempt with "([^"]*)"$`, c.theNodeFailsOnEveryAttemptWith)
	ctx.Step(`^the node executes (\d+) times total$`, c.theNodeExecutesTimesTotal)
	ctx.Step(`^the resilient DAG execution fails with "([^"]*)"$`, c.theResilientDAGExecutionFailsWith)

	ctx.Step(`^a DAG node configured with a (\d+) millisecond timeout$`, c.aDAGNodeConfiguredWithAMillisecondTimeout)
	ctx.Step(`^the node step takes (\d+) milliseconds to complete$`, c.theNodeStepTakesMillisecondsToComplete)
	ctx.Step(`^the resilient DAG execution fails with deadline exceeded error$`, c.theResilientDAGExecutionFailsWithDeadlineExceededError)

	ctx.Step(`^3 racing steps where step 1 finishes in 10ms, step 2 in 100ms, and step 3 in 200ms$`, c.threeRacingStepsWithDurations)
	ctx.Step(`^the racing steps are executed$`, c.theRacingStepsAreExecuted)
	ctx.Step(`^the fastest step wins$`, c.theFastestStepWins)
	ctx.Step(`^losing branches are cancelled$`, c.losingBranchesAreCancelled)
	ctx.Step(`^the race execution succeeds$`, c.theRaceExecutionSucceeds)

	ctx.Step(`^(\d+) racing steps that all fail with error "([^"]*)"$`, c.racingStepsThatAllFailWithError)
	ctx.Step(`^the race execution fails$`, c.theRaceExecutionFails)

	ctx.Step(`^a race fallback step that succeeds$`, c.aRaceFallbackStepThatSucceeds)
	ctx.Step(`^the racing steps with fallback are executed$`, c.theRacingStepsWithFallbackAreExecuted)
	ctx.Step(`^the race fallback step executes$`, c.theRaceFallbackStepExecutes)
	ctx.Step(`^the race execution with fallback succeeds$`, c.theRaceExecutionWithFallbackSucceeds)
}
