package flow

import (
	"context"
	"iter"
)

func Pipe[T context.Context, In, Out any](
	fn func(ctx T, in In) (Out, error),
	get func(ctx T) In,
	set func(ctx T, out Out),
) Step[T] {
	return func(ctx T) error {
		if fn == nil {
			return nil
		}
		var in In
		if get != nil {
			in = get(ctx)
		}
		out, err := fn(ctx, in)
		if err != nil {
			return err
		}
		if set != nil {
			set(ctx, out)
		}
		return nil
	}
}

func PipeSeq[T context.Context, In, Out any](
	seq iter.Seq[In],
	transform func(ctx T, item In) (Out, error),
	sink func(ctx T, item Out) error,
) Step[T] {
	return func(ctx T) error {
		if seq == nil || transform == nil || sink == nil {
			return nil
		}
		for item := range seq {
			out, err := transform(ctx, item)
			if err != nil {
				return err
			}
			if err := sink(ctx, out); err != nil {
				return err
			}
		}
		return nil
	}
}

func PipeSeq2[T context.Context, K, V, Out any](
	seq iter.Seq2[K, V],
	transform func(ctx T, key K, val V) (Out, error),
	sink func(ctx T, item Out) error,
) Step[T] {
	return func(ctx T) error {
		if seq == nil || transform == nil || sink == nil {
			return nil
		}
		for k, v := range seq {
			out, err := transform(ctx, k, v)
			if err != nil {
				return err
			}
			if err := sink(ctx, out); err != nil {
				return err
			}
		}
		return nil
	}
}

func Pipe2[T context.Context, A, B, C any](
	f1 func(ctx T, a A) (B, error),
	f2 func(ctx T, b B) (C, error),
) func(ctx T, a A) (C, error) {
	return func(ctx T, a A) (C, error) {
		var zero C
		if f1 == nil {
			if f2 == nil {
				return zero, nil
			}
			var b B
			return f2(ctx, b)
		}
		b, err := f1(ctx, a)
		if err != nil {
			return zero, err
		}
		if f2 == nil {
			return zero, nil
		}
		return f2(ctx, b)
	}
}

func Pipe3[T context.Context, A, B, C, D any](
	f1 func(ctx T, a A) (B, error),
	f2 func(ctx T, b B) (C, error),
	f3 func(ctx T, c C) (D, error),
) func(ctx T, a A) (D, error) {
	return func(ctx T, a A) (D, error) {
		var zero D
		p := Pipe2(f1, f2)
		c, err := p(ctx, a)
		if err != nil {
			return zero, err
		}
		if f3 == nil {
			return zero, nil
		}
		return f3(ctx, c)
	}
}
