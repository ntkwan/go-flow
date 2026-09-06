package experiments_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ntkwan/go-flow"
)

type LatencyStats struct {
	P50  time.Duration
	P90  time.Duration
	P95  time.Duration
	P99  time.Duration
	P999 time.Duration
	Mean time.Duration
	Max  time.Duration
}

func computePercentiles(durations []time.Duration) LatencyStats {
	if len(durations) == 0 {
		return LatencyStats{}
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}

	n := len(sorted)
	return LatencyStats{
		P50:  sorted[int(float64(n)*0.50)],
		P90:  sorted[int(float64(n)*0.90)],
		P95:  sorted[int(float64(n)*0.95)],
		P99:  sorted[int(float64(n)*0.99)],
		P999: sorted[int(float64(n)*0.999)],
		Mean: sum / time.Duration(n),
		Max:  sorted[n-1],
	}
}

type queryContext struct {
	context.Context
	replicaID int
	response  string
	mu        sync.Mutex
}

func simulateReplicaCall(replicaID int, seed uint64) flow.Step[*queryContext] {
	return func(ctx *queryContext) error {
		r := rand.New(rand.NewPCG(seed, uint64(replicaID)))

		var sleepTime time.Duration
		isTailLatency := r.Float64() < 0.05
		if isTailLatency {
			sleepTime = time.Duration(30+r.IntN(40)) * time.Millisecond
		} else {
			sleepTime = time.Duration(1+r.IntN(3)) * time.Millisecond
		}

		select {
		case <-time.After(sleepTime):
			ctx.mu.Lock()
			if ctx.response == "" {
				ctx.replicaID = replicaID
				ctx.response = "ok"
			}
			ctx.mu.Unlock()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func TestHedgedRequestsSpeculativeRacing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping hedged requests test in short mode")
	}

	const iterations = 1000

	sequentialLatencies := make([]time.Duration, iterations)
	for i := range iterations {
		reqCtx := &queryContext{Context: context.Background()}
		start := time.Now()

		step1 := simulateReplicaCall(1, uint64(i*10+1)).Timeout(10 * time.Millisecond)
		step2 := simulateReplicaCall(2, uint64(i*10+2)).Timeout(10 * time.Millisecond)
		step3 := simulateReplicaCall(3, uint64(i*10+3)).Timeout(10 * time.Millisecond)

		fallbackPipeline := step1.Fallback(step2.Fallback(step3))
		_ = fallbackPipeline.Exec(reqCtx)

		sequentialLatencies[i] = time.Since(start)
	}

	hedgedLatencies := make([]time.Duration, iterations)
	for i := range iterations {
		reqCtx := &queryContext{Context: context.Background()}
		start := time.Now()

		step1 := simulateReplicaCall(1, uint64(i*10+1))
		step2 := simulateReplicaCall(2, uint64(i*10+2))
		step3 := simulateReplicaCall(3, uint64(i*10+3))

		raceStep := flow.Race(step1, step2, step3)
		if err := raceStep.Exec(reqCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected race failure: %v", err)
		}

		hedgedLatencies[i] = time.Since(start)
	}

	seqStats := computePercentiles(sequentialLatencies)
	hedgedStats := computePercentiles(hedgedLatencies)

	t.Logf("Sequential Fallback: p50=%v, p90=%v, p95=%v, p99=%v, p99.9=%v, Mean=%v, Max=%v",
		seqStats.P50, seqStats.P90, seqStats.P95, seqStats.P99, seqStats.P999, seqStats.Mean, seqStats.Max)
	t.Logf("Hedged Race:        p50=%v, p90=%v, p95=%v, p99=%v, p99.9=%v, Mean=%v, Max=%v",
		hedgedStats.P50, hedgedStats.P90, hedgedStats.P95, hedgedStats.P99, hedgedStats.P999, hedgedStats.Mean, hedgedStats.Max)

	if hedgedStats.P99 >= seqStats.P99 {
		t.Logf("Notice: hedged p99 (%v) vs sequential p99 (%v)", hedgedStats.P99, seqStats.P99)
	}
}

func BenchmarkHedgedRaceVsSequential(b *testing.B) {
	b.Run("SequentialFallback", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			reqCtx := &queryContext{Context: context.Background()}
			s1 := simulateReplicaCall(1, 42).Timeout(5 * time.Millisecond)
			s2 := simulateReplicaCall(2, 43).Timeout(5 * time.Millisecond)
			_ = s1.Fallback(s2).Exec(reqCtx)
		}
	})

	b.Run("SpeculativeRace", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			reqCtx := &queryContext{Context: context.Background()}
			s1 := simulateReplicaCall(1, 42)
			s2 := simulateReplicaCall(2, 43)
			_ = flow.Race(s1, s2).Exec(reqCtx)
		}
	})
}
