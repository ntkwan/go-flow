package fuzz_test

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/ntkwan/go-flow"
)

func FuzzPipeSeq(f *testing.F) {
	f.Add([]byte("hello world"))
	f.Add([]byte(""))
	f.Add([]byte("abc\x00def\xff"))

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 500 {
			input = input[:500]
		}

		seq := slices.Values(input)
		var out bytes.Buffer

		step := flow.PipeSeq(
			seq,
			func(ctx context.Context, b byte) (byte, error) {
				return b ^ 0x5A, nil
			},
			func(ctx context.Context, b byte) error {
				out.WriteByte(b)
				return nil
			},
		)

		err := step(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if out.Len() != len(input) {
			t.Fatalf("expected len %d, got %d", len(input), out.Len())
		}

		result := out.Bytes()
		for i, b := range input {
			if result[i] != (b ^ 0x5A) {
				t.Fatalf("mismatch at index %d", i)
			}
		}
	})
}
