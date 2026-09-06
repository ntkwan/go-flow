package experiments_test

import (
	"context"
	"fmt"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntkwan/go-flow"
)

type RawRecord struct {
	ID        int64
	Timestamp int64
	Data      string
}

type ProcessedRecord struct {
	ID        int64
	Hash      string
	Valid     bool
	Processed int64
}

type ETLMetrics struct {
	Duration     time.Duration
	Throughput   float64
	TotalAllocMB float64
	NumGC        uint32
}

func generateStream(total int) iter.Seq[RawRecord] {
	return func(yield func(RawRecord) bool) {
		for i := range total {
			rec := RawRecord{
				ID:        int64(i + 1),
				Timestamp: time.Now().UnixNano(),
				Data:      "payload-data-record",
			}
			if !yield(rec) {
				return
			}
		}
	}
}

func transformRecord(r RawRecord) ProcessedRecord {
	return ProcessedRecord{
		ID:        r.ID,
		Hash:      fmt.Sprintf("hash-%d", r.ID),
		Valid:     r.ID%2 == 0,
		Processed: time.Now().UnixNano(),
	}
}

func runIteratorETL(total int, workers int, chunkSize int) (ETLMetrics, error) {
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	start := time.Now()
	var processedCount atomic.Int64

	ctx := context.Background()
	recordsSeq := generateStream(total)

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	step := flow.Chunk(recordsSeq, chunkSize, func(c context.Context, batch []RawRecord) error {
		sem <- struct{}{}
		wg.Add(1)
		go func(b []RawRecord) {
			defer func() {
				<-sem
				wg.Done()
			}()
			for _, r := range b {
				_ = transformRecord(r)
			}
			processedCount.Add(int64(len(b)))
		}(batch)
		return nil
	})

	if err := step(ctx); err != nil {
		return ETLMetrics{}, err
	}
	wg.Wait()

	duration := time.Since(start)
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	return ETLMetrics{
		Duration:     duration,
		Throughput:   float64(processedCount.Load()) / duration.Seconds(),
		TotalAllocMB: float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1024 * 1024),
		NumGC:        memAfter.NumGC - memBefore.NumGC,
	}, nil
}

func runChannelETL(total int, workers int, chunkSize int) (ETLMetrics, error) {
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	start := time.Now()
	var processedCount atomic.Int64

	batchChan := make(chan []RawRecord, workers*2)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchChan {
				for _, r := range batch {
					_ = transformRecord(r)
				}
				processedCount.Add(int64(len(batch)))
			}
		}()
	}

	currentBatch := make([]RawRecord, 0, chunkSize)
	for i := range total {
		rec := RawRecord{
			ID:        int64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Data:      "payload-data-record",
		}
		currentBatch = append(currentBatch, rec)
		if len(currentBatch) == chunkSize {
			batchChan <- currentBatch
			currentBatch = make([]RawRecord, 0, chunkSize)
		}
	}
	if len(currentBatch) > 0 {
		batchChan <- currentBatch
	}
	close(batchChan)

	wg.Wait()

	duration := time.Since(start)
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	return ETLMetrics{
		Duration:     duration,
		Throughput:   float64(processedCount.Load()) / duration.Seconds(),
		TotalAllocMB: float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1024 * 1024),
		NumGC:        memAfter.NumGC - memBefore.NumGC,
	}, nil
}

func TestStreamingETLComparison(t *testing.T) {
	const recordCount = 1000000
	const workers = 8
	const chunkSize = 256

	iterMetrics, err := runIteratorETL(recordCount, workers, chunkSize)
	if err != nil {
		t.Fatalf("iterator ETL failed: %v", err)
	}

	chanMetrics, err := runChannelETL(recordCount, workers, chunkSize)
	if err != nil {
		t.Fatalf("channel ETL failed: %v", err)
	}

	t.Logf("Iterator (Go-Flow): Duration=%v, Throughput=%.0f rec/sec, Alloc=%.2f MB, NumGC=%d",
		iterMetrics.Duration, iterMetrics.Throughput, iterMetrics.TotalAllocMB, iterMetrics.NumGC)
	t.Logf("Channel (Native):  Duration=%v, Throughput=%.0f rec/sec, Alloc=%.2f MB, NumGC=%d",
		chanMetrics.Duration, chanMetrics.Throughput, chanMetrics.TotalAllocMB, chanMetrics.NumGC)

	if iterMetrics.Throughput <= 0 || chanMetrics.Throughput <= 0 {
		t.Fatalf("expected positive throughput")
	}
}

func BenchmarkETLIterator(b *testing.B) {
	const count = 100000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = runIteratorETL(count, 8, 256)
	}
}

func BenchmarkETLChannel(b *testing.B) {
	const count = 100000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = runChannelETL(count, 8, 256)
	}
}
