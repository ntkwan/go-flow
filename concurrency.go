package flow

import (
	"context"
	"errors"
	"iter"
	"sync"
)

func GoN[T context.Context](limit int, steps ...Step[T]) Step[T] {
	if len(steps) == 0 {
		return func(ctx T) error { return nil }
	}
	if limit <= 0 || limit >= len(steps) {
		return Go(steps...)
	}
	return func(ctx T) error {
		errs := make([]error, len(steps))
		var wg sync.WaitGroup
		sem := make(chan struct{}, limit)
		for i, step := range steps {
			sem <- struct{}{}
			wg.Add(1)
			go func(idx int, s Step[T]) {
				defer func() {
					<-sem
					wg.Done()
				}()
				errs[idx] = s(ctx)
			}(i, step)
		}
		wg.Wait()
		return errors.Join(errs...)
	}
}

func Race[T context.Context](steps ...Step[T]) Step[T] {
	return func(ctx T) error {
		if len(steps) == 0 {
			return nil
		}
		raceCtx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)

		stepCtx := ctx
		if c, ok := any(raceCtx).(T); ok {
			stepCtx = c
		}

		resultCh := make(chan error, len(steps))
		for _, step := range steps {
			s := step
			go func() {
				resultCh <- s(stepCtx)
			}()
		}

		var errs []error
		for range steps {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case err := <-resultCh:
				if err == nil {
					cancel(nil)
					return nil
				}
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
}

func Each[T context.Context, V any](seq iter.Seq[V], step func(ctx T, item V) error) Step[T] {
	return func(ctx T) error {
		if seq == nil || step == nil {
			return nil
		}
		for item := range seq {
			if err := step(ctx, item); err != nil {
				return err
			}
		}
		return nil
	}
}

func Each2[T context.Context, K, V any](seq iter.Seq2[K, V], step func(ctx T, key K, val V) error) Step[T] {
	return func(ctx T) error {
		if seq == nil || step == nil {
			return nil
		}
		for k, v := range seq {
			if err := step(ctx, k, v); err != nil {
				return err
			}
		}
		return nil
	}
}

func Once[T context.Context](step Step[T]) Step[T] {
	if step == nil {
		return func(ctx T) error { return nil }
	}
	var (
		once sync.Once
		err  error
	)
	return func(ctx T) error {
		once.Do(func() {
			err = step(ctx)
		})
		return err
	}
}

func Chunk[T context.Context, V any](seq iter.Seq[V], size int, step func(ctx T, batch []V) error) Step[T] {
	return func(ctx T) error {
		if seq == nil || step == nil {
			return nil
		}
		if size <= 0 {
			size = 1
		}
		batch := make([]V, 0, size)
		for item := range seq {
			batch = append(batch, item)
			if len(batch) == size {
				if err := step(ctx, batch); err != nil {
					return err
				}
				batch = make([]V, 0, size)
			}
		}
		if len(batch) > 0 {
			if err := step(ctx, batch); err != nil {
				return err
			}
		}
		return nil
	}
}

func Chunk2[T context.Context, K, V any](seq iter.Seq2[K, V], size int, step func(ctx T, keys []K, vals []V) error) Step[T] {
	return func(ctx T) error {
		if seq == nil || step == nil {
			return nil
		}
		if size <= 0 {
			size = 1
		}
		keys := make([]K, 0, size)
		vals := make([]V, 0, size)
		for k, v := range seq {
			keys = append(keys, k)
			vals = append(vals, v)
			if len(keys) == size {
				if err := step(ctx, keys, vals); err != nil {
					return err
				}
				keys = make([]K, 0, size)
				vals = make([]V, 0, size)
			}
		}
		if len(keys) > 0 {
			if err := step(ctx, keys, vals); err != nil {
				return err
			}
		}
		return nil
	}
}
