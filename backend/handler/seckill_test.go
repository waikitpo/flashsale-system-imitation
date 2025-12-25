package handler_test

import (
	"seckillapp/handler"
	"testing"
	"time"
	"unsafe"
)

func init() {
	// Initialize C++ engine once for all benchmarks
	handler.StartConsumer()
	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)
}

// Single-threaded SPSC Benchmark
func BenchmarkSPSCEnqueue(b *testing.B) {
	handler.EnableCorrectnessCheck(false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.BenchmarkEnqueue()
	}
}

// Batch Benchmark
func BenchmarkBatchEnqueue(b *testing.B) {
	handler.EnableCorrectnessCheck(false)

	// Prepare batch buffer
	batchSize := 128

	// Match C struct layout: int64, int32, padding(4), uint64, uint64
	type CRequest struct {
		SkuID     int64
		Qty       int32
		_         int32 // padding
		GuestID   uint64
		RequestID uint64
	}

	reqs := make([]CRequest, batchSize)
	for i := 0; i < batchSize; i++ {
		reqs[i].SkuID = 123
		reqs[i].Qty = 1
		reqs[i].GuestID = 1001
		reqs[i].RequestID = uint64(i)
	}

	ptr := unsafe.Pointer(&reqs[0])

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		handler.EnqueueBatchRaw(ptr, batchSize)
	}

	b.StopTimer()
	nsPerMsg := float64(b.Elapsed().Nanoseconds()) / float64(b.N*batchSize)
	b.ReportMetric(nsPerMsg, "ns/msg")
}

func TestCorrectness(t *testing.T) {
	handler.EnableCorrectnessCheck(true)
	// Give consumer time to reset
	time.Sleep(100 * time.Millisecond)

	count := 100000

	// Enqueue 1..N
	for i := 1; i <= count; i++ {
		handler.EnqueueWithSeq(uint64(i))
	}

	// Wait for drain (consumer is polling)
	// With 100k items, it should be very fast (<10ms), but sleep longer to be safe
	time.Sleep(500 * time.Millisecond)

	lastSeq, gap, dup := handler.GetConsumerStats()

	t.Logf("Correctness Check: Sent=%d, LastSeq=%d, Gap=%d, Dup=%d", count, lastSeq, gap, dup)

	if lastSeq != uint64(count) {
		t.Errorf("Expected last seq %d, got %d. (Maybe queue full dropped some?)", count, lastSeq)
	}
	if gap != 0 {
		t.Errorf("Gap count %d, expected 0. (Dropped items?)", gap)
	}
	if dup != 0 {
		t.Errorf("Dup count %d, expected 0. (Duplicate processing?)", dup)
	}
}
