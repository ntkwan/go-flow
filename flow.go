// Package flow provides structured concurrency and workflow pipelines for flow.
package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Step represents Step.
type Step[T context.Context] func(ctx T) error

// Seq performs the Seq operation.
func Seq[T context.Context](steps ...Step[T]) Step[T] {
	return func(ctx T) error {
		for _, step := range steps {
			if err := step(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

// Go performs the Go operation.
func Go[T context.Context](steps ...Step[T]) Step[T] {
	return func(ctx T) error {
		if len(steps) == 0 {
			return nil
		}
		errs := make([]error, len(steps))
		var hasErr atomic.Bool
		var wg sync.WaitGroup
		wg.Add(len(steps))
		for i, step := range steps {
			go func(idx int, s Step[T]) {
				defer wg.Done()
				if s != nil {
					if err := s(ctx); err != nil {
						errs[idx] = err
						hasErr.Store(true)
					}
				}
			}(i, step)
		}
		wg.Wait()
		if !hasErr.Load() {
			return nil
		}
		return errors.Join(errs...)
	}
}

// Branch performs the Branch operation.
func Branch[T context.Context](condition func(ctx T) bool, ifBranch, elseBranch Step[T]) Step[T] {
	return func(ctx T) error {
		if condition != nil && condition(ctx) {
			if ifBranch != nil {
				return ifBranch(ctx)
			}
			return nil
		}
		if elseBranch != nil {
			return elseBranch(ctx)
		}
		return nil
	}
}

// Dynamic performs the Dynamic operation.
func Dynamic[T context.Context](fn func(ctx T) Step[T]) Step[T] {
	return func(ctx T) error {
		if fn == nil {
			return nil
		}
		step := fn(ctx)
		if step == nil {
			return nil
		}
		return step(ctx)
	}
}

// Exec executes the Exec operation.
func (s Step[T]) Exec(ctx T) error {
	if s == nil {
		return nil
	}
	return s(ctx)
}

// Then executes the Then operation.
func (s Step[T]) Then(steps ...Step[T]) Step[T] {
	all := make([]Step[T], 0, 1+len(steps))
	if s != nil {
		all = append(all, s)
	}
	all = append(all, steps...)
	return Seq(all...)
}

// Go executes the Go operation.
func (s Step[T]) Go(steps ...Step[T]) Step[T] {
	all := make([]Step[T], 0, 1+len(steps))
	if s != nil {
		all = append(all, s)
	}
	all = append(all, steps...)
	return Go(all...)
}

// GoN executes the GoN operation.
func (s Step[T]) GoN(limit int, steps ...Step[T]) Step[T] {
	all := make([]Step[T], 0, 1+len(steps))
	if s != nil {
		all = append(all, s)
	}
	all = append(all, steps...)
	return GoN(limit, all...)
}

// Race executes the Race operation.
func (s Step[T]) Race(steps ...Step[T]) Step[T] {
	all := make([]Step[T], 0, 1+len(steps))
	if s != nil {
		all = append(all, s)
	}
	all = append(all, steps...)
	return Race(all...)
}

// Once executes the Once operation.
func (s Step[T]) Once() Step[T] {
	return Once(s)
}

// Timeout executes the Timeout operation.
func (s Step[T]) Timeout(d time.Duration) Step[T] {
	return func(ctx T) error {
		if s == nil {
			return nil
		}
		timeoutCtx, cancel := context.WithTimeout(ctx, d)
		defer cancel()

		stepCtx := ctx
		if c, ok := any(timeoutCtx).(T); ok {
			stepCtx = c
		}

		done := make(chan error, 1)
		go func() {
			done <- s(stepCtx)
		}()

		select {
		case <-timeoutCtx.Done():
			return timeoutCtx.Err()
		case err := <-done:
			return err
		}
	}
}

// Retry executes the Retry operation.
func (s Step[T]) Retry(attempts int, delay time.Duration) Step[T] {
	return func(ctx T) error {
		if s == nil {
			return nil
		}
		if attempts <= 0 {
			attempts = 1
		}
		var err error
		for i := 0; i < attempts; i++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			err = s(ctx)
			if err == nil {
				return nil
			}
			if i < attempts-1 && delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
		return err
	}
}

// Fallback executes the Fallback operation.
func (s Step[T]) Fallback(fallback Step[T]) Step[T] {
	return func(ctx T) error {
		if s == nil {
			if fallback != nil {
				return fallback(ctx)
			}
			return nil
		}
		if err := s(ctx); err != nil {
			if fallback != nil {
				return fallback(ctx)
			}
			return err
		}
		return nil
	}
}

// Catch executes the Catch operation.
func (s Step[T]) Catch(handler func(ctx T, err error) error) Step[T] {
	return func(ctx T) error {
		if s == nil {
			return nil
		}
		if err := s(ctx); err != nil {
			if handler != nil {
				return handler(ctx, err)
			}
			return err
		}
		return nil
	}
}

// Recover executes the Recover operation.
func (s Step[T]) Recover() Step[T] {
	return func(ctx T) (err error) {
		if s == nil {
			return nil
		}
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					err = fmt.Errorf("panic recovered: %w", e)
				} else {
					err = fmt.Errorf("panic recovered: %v", r)
				}
			}
		}()
		return s(ctx)
	}
}

// When executes the When operation.
func (s Step[T]) When(predicate func(ctx T) bool) Step[T] {
	return func(ctx T) error {
		if s == nil || (predicate != nil && !predicate(ctx)) {
			return nil
		}
		return s(ctx)
	}
}

// Unless executes the Unless operation.
func (s Step[T]) Unless(predicate func(ctx T) bool) Step[T] {
	return func(ctx T) error {
		if s == nil || (predicate != nil && predicate(ctx)) {
			return nil
		}
		return s(ctx)
	}
}

// Branch executes the Branch operation.
func (s Step[T]) Branch(condition func(ctx T) bool, ifBranch, elseBranch Step[T]) Step[T] {
	return s.Then(Branch(condition, ifBranch, elseBranch))
}

// Dynamic executes the Dynamic operation.
func (s Step[T]) Dynamic(fn func(ctx T) Step[T]) Step[T] {
	return s.Then(Dynamic(fn))
}

// Middleware represents Middleware.
type Middleware[T context.Context] func(next Step[T]) Step[T]

// Wrap executes the Wrap operation.
func (s Step[T]) Wrap(middlewares ...Middleware[T]) Step[T] {
	if s == nil {
		return func(ctx T) error { return nil }
	}
	wrapped := s
	for i := len(middlewares) - 1; i >= 0; i-- {
		if mw := middlewares[i]; mw != nil {
			wrapped = mw(wrapped)
		}
	}
	return wrapped
}
