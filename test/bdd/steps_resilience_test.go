package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cucumber/godog"

	"github.com/ntkwan/go-flow"
)

type customErr struct {
	msg string
}

func (e *customErr) Error() string {
	return e.msg
}

type resilienceContext struct {
	guardedStep        flow.Step[context.Context]
	resilienceErr      error
	cancelledCtx       context.Context
	cancelFunc         context.CancelFunc
	stepToCheckCtx     flow.Step[context.Context]
	timeoutStep        flow.Step[context.Context]
	timeoutDuration    time.Duration
	stepDuration       time.Duration
	errA               error
	errB               error
	multiErrParallel   flow.Step[context.Context]
	parallelResultsErr error
}

func newResilienceContext() *resilienceContext {
	return &resilienceContext{}
}

func (c *resilienceContext) aStepThatFailsWith(errMsg string) error {
	c.guardedStep = flow.Step[context.Context](func(ctx context.Context) error {
		return errors.New(errMsg)
	})
	return nil
}

func (c *resilienceContext) aCatchHandlerThatSuppressesTheError() error {
	c.guardedStep = c.guardedStep.Catch(func(ctx context.Context, err error) error {
		return nil
	})
	return nil
}

func (c *resilienceContext) aCatchHandlerThatTransformsItTo(newErrMsg string) error {
	c.guardedStep = c.guardedStep.Catch(func(ctx context.Context, err error) error {
		return errors.New(newErrMsg)
	})
	return nil
}

func (c *resilienceContext) theGuardedStepIsExecuted() error {
	c.resilienceErr = c.guardedStep(context.Background())
	return nil
}

func (c *resilienceContext) theGuardedExecutionSucceeds() error {
	if c.resilienceErr != nil {
		return fmt.Errorf("expected success, got: %w", c.resilienceErr)
	}
	return nil
}

func (c *resilienceContext) theExecutionFailsWith(errMsg string) error {
	if c.resilienceErr == nil {
		return errors.New("expected error, got nil")
	}
	if !strings.Contains(c.resilienceErr.Error(), errMsg) {
		return fmt.Errorf("expected error containing %q, got: %w", errMsg, c.resilienceErr)
	}
	return nil
}

func (c *resilienceContext) aStepThatPanicsWith(panicVal string) error {
	c.guardedStep = flow.Step[context.Context](func(ctx context.Context) error {
		panic(panicVal)
	})
	return nil
}

func (c *resilienceContext) aRecoverGuardAttachedToTheStep() error {
	c.guardedStep = c.guardedStep.Recover()
	return nil
}

func (c *resilienceContext) aRecoveryMiddlewareWrappingTheStep() error {
	c.guardedStep = c.guardedStep.Wrap(flow.Recovery[context.Context]())
	return nil
}

func (c *resilienceContext) theExecutionFailsWithAPanicErrorContaining(panicVal string) error {
	if c.resilienceErr == nil {
		return errors.New("expected panic error, got nil")
	}
	if !strings.Contains(c.resilienceErr.Error(), panicVal) || !strings.Contains(c.resilienceErr.Error(), "panic recovered") {
		return fmt.Errorf("expected panic recovered message containing %q, got: %w", panicVal, c.resilienceErr)
	}
	return nil
}

func (c *resilienceContext) noPanicCrashesTheProcess() error {
	return nil
}

func (c *resilienceContext) twoConcurrentStepsFailingWithAnd(err1, err2 string) error {
	c.errA = &customErr{msg: err1}
	c.errB = &customErr{msg: err2}
	c.multiErrParallel = flow.Go(
		func(ctx context.Context) error { return c.errA },
		func(ctx context.Context) error { return c.errB },
	)
	return nil
}

func (c *resilienceContext) theMultiErrorFlowIsExecuted() error {
	c.parallelResultsErr = c.multiErrParallel(context.Background())
	return nil
}

func (c *resilienceContext) theResultingErrorUnwrapsTo(targetErr string) error {
	if c.parallelResultsErr == nil {
		return errors.New("expected error, got nil")
	}
	if !errors.Is(c.parallelResultsErr, c.errA) && !errors.Is(c.parallelResultsErr, c.errB) {
		return fmt.Errorf("error %v does not match target error %q via errors.Is", c.parallelResultsErr, targetErr)
	}
	return nil
}

func (c *resilienceContext) aContextThatIsAlreadyCancelled() error {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.cancelledCtx = ctx
	c.cancelFunc = cancel
	return nil
}

func (c *resilienceContext) aStepCheckingContextCancellation() error {
	c.stepToCheckCtx = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	return nil
}

func (c *resilienceContext) theStepIsExecutedWithTheCancelledContext() error {
	c.resilienceErr = c.stepToCheckCtx(c.cancelledCtx)
	return nil
}

func (c *resilienceContext) theStepAbortsImmediatelyWithContextCancelledError() error {
	if c.resilienceErr == nil {
		return errors.New("expected context.Canceled error, got nil")
	}
	if !errors.Is(c.resilienceErr, context.Canceled) {
		return fmt.Errorf("expected context.Canceled, got: %w", c.resilienceErr)
	}
	return nil
}

func (c *resilienceContext) aStepConfiguredWithAMillisecondTimeout(timeoutMs int) error {
	c.timeoutDuration = time.Duration(timeoutMs) * time.Millisecond
	return nil
}

func (c *resilienceContext) theStepOperationTakesMilliseconds(durationMs int) error {
	c.stepDuration = time.Duration(durationMs) * time.Millisecond
	base := flow.Step[context.Context](func(ctx context.Context) error {
		select {
		case <-time.After(c.stepDuration):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	c.timeoutStep = base.Timeout(c.timeoutDuration)
	return nil
}

func (c *resilienceContext) theTimedStepIsExecuted() error {
	c.resilienceErr = c.timeoutStep(context.Background())
	return nil
}

func (c *resilienceContext) theStepReturnsDeadlineExceededError() error {
	if c.resilienceErr == nil {
		return errors.New("expected deadline error, got nil")
	}
	if !errors.Is(c.resilienceErr, context.DeadlineExceeded) && !strings.Contains(c.resilienceErr.Error(), "context deadline exceeded") {
		return fmt.Errorf("expected deadline exceeded, got: %w", c.resilienceErr)
	}
	return nil
}

func registerResilienceSteps(ctx *godog.ScenarioContext) {
	c := newResilienceContext()

	ctx.Step(`^a step that fails with "([^"]*)"$`, c.aStepThatFailsWith)
	ctx.Step(`^a Catch handler that suppresses the error$`, c.aCatchHandlerThatSuppressesTheError)
	ctx.Step(`^a Catch handler that transforms it to "([^"]*)"$`, c.aCatchHandlerThatTransformsItTo)
	ctx.Step(`^the guarded step is executed$`, c.theGuardedStepIsExecuted)
	ctx.Step(`^the guarded execution succeeds$`, c.theGuardedExecutionSucceeds)
	ctx.Step(`^the execution fails with "([^"]*)"$`, c.theExecutionFailsWith)

	ctx.Step(`^a step that panics with "([^"]*)"$`, c.aStepThatPanicsWith)
	ctx.Step(`^a Recover guard attached to the step$`, c.aRecoverGuardAttachedToTheStep)
	ctx.Step(`^a Recovery middleware wrapping the step$`, c.aRecoveryMiddlewareWrappingTheStep)
	ctx.Step(`^the execution fails with a panic error containing "([^"]*)"$`, c.theExecutionFailsWithAPanicErrorContaining)
	ctx.Step(`^no panic crashes the process$`, c.noPanicCrashesTheProcess)

	ctx.Step(`^(\d+) concurrent steps failing with "([^"]*)" and "([^"]*)"$`, c.twoConcurrentStepsFailingWithAnd)
	ctx.Step(`^the multi-error flow is executed$`, c.theMultiErrorFlowIsExecuted)
	ctx.Step(`^the resulting error unwraps to "([^"]*)"$`, c.theResultingErrorUnwrapsTo)

	ctx.Step(`^a context that is already cancelled$`, c.aContextThatIsAlreadyCancelled)
	ctx.Step(`^a step checking context cancellation$`, c.aStepCheckingContextCancellation)
	ctx.Step(`^the step is executed with the cancelled context$`, c.theStepIsExecutedWithTheCancelledContext)
	ctx.Step(`^the step aborts immediately with context cancelled error$`, c.theStepAbortsImmediatelyWithContextCancelledError)

	ctx.Step(`^a step configured with a (\d+) millisecond timeout$`, c.aStepConfiguredWithAMillisecondTimeout)
	ctx.Step(`^the step operation takes (\d+) milliseconds$`, c.theStepOperationTakesMilliseconds)
	ctx.Step(`^the timed step is executed$`, c.theTimedStepIsExecuted)
	ctx.Step(`^the step returns deadline exceeded error$`, c.theStepReturnsDeadlineExceededError)
}
