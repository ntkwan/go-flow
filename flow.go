package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Step[T context.Context] func(ctx T) error

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

func Go[T context.Context](steps ...Step[T]) Step[T] {
	return func(ctx T) error {
		if len(steps) == 0 {
			return nil
		}
		errs := make([]error, len(steps))
		var wg sync.WaitGroup
		for i, step := range steps {
			idx, s := i, step
			wg.Go(func() {
				errs[idx] = s(ctx)
			})
		}
		wg.Wait()
		return errors.Join(errs...)
	}
}

func (s Step[T]) Exec(ctx T) error {
	if s == nil {
		return nil
	}
	return s(ctx)
}

func (s Step[T]) Then(steps ...Step[T]) Step[T] {
	all := make([]Step[T], 0, 1+len(steps))
	if s != nil {
		all = append(all, s)
	}
	all = append(all, steps...)
	return Seq(all...)
}

func (s Step[T]) Go(steps ...Step[T]) Step[T] {
	all := make([]Step[T], 0, 1+len(steps))
	if s != nil {
		all = append(all, s)
	}
	all = append(all, steps...)
	return Go(all...)
}

func (s Step[T]) GoN(limit int, steps ...Step[T]) Step[T] {
	all := make([]Step[T], 0, 1+len(steps))
	if s != nil {
		all = append(all, s)
	}
	all = append(all, steps...)
	return GoN(limit, all...)
}

func (s Step[T]) Race(steps ...Step[T]) Step[T] {
	all := make([]Step[T], 0, 1+len(steps))
	if s != nil {
		all = append(all, s)
	}
	all = append(all, steps...)
	return Race(all...)
}

func (s Step[T]) Once() Step[T] {
	return Once(s)
}

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
		var wg sync.WaitGroup
		wg.Go(func() {
			done <- s(stepCtx)
		})

		select {
		case <-timeoutCtx.Done():
			return timeoutCtx.Err()
		case err := <-done:
			return err
		}
	}
}

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
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			}
		}
		return err
	}
}

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

func (s Step[T]) When(predicate func(ctx T) bool) Step[T] {
	return func(ctx T) error {
		if s == nil || (predicate != nil && !predicate(ctx)) {
			return nil
		}
		return s(ctx)
	}
}

func (s Step[T]) Unless(predicate func(ctx T) bool) Step[T] {
	return func(ctx T) error {
		if s == nil || (predicate != nil && predicate(ctx)) {
			return nil
		}
		return s(ctx)
	}
}
